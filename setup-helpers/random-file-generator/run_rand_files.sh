#!/bin/bash

# This script simply orchestrates the necessary steps
# need to run `rand_files.py` on the head node of a
# cluster.
source .miniconda3/etc/profile.d/conda.sh
conda activate cloud-data

cd cloud-data-transfer-benchmarking/random-file-generator
python -u rand_files.py # Use `-u` flag to disable output buffering

scp -q /home/jgreen/cloud-data-transfer-benchmarking/inputs/inputs.json usercontainer:${LOCALDIR} # Send amended benchmark info file back to user container