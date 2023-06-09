#!/bin/bash

# This script simply orchestrates the necessary steps
# need to run `rand_files.py` on the head node of a
# cluster.
source .miniconda3/etc/profile.d/conda.sh
conda activate cloud-data
cd random-file-generator
python rand_files.py
scp -q benchmark_info.json usercontainer:${LOCALDIR} # Send amended benchmark info file back to user container