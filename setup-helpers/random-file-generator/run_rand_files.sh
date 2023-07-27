#!/bin/bash

# This script simply orchestrates the necessary steps
# need to run `rand_files.py` on the head node of a
# cluster.
source .miniconda3/etc/profile.d/conda.sh
conda activate cloud-data

cd random-file-generator
python -u rand_files.py # Use `-u` flag to disable output buffering

scp -q benchmark_info.json usercontainer:${LOCALDIR} # Send amended benchmark info file back to user container

# Remove AWS credentials
if [ -d "${HOME}/.aws" ]
then
    rm -r ${HOME}/.aws
fi