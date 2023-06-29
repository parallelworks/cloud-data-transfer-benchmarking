#!/bin/bash

# This script simply orchestrates the necessary steps
# need to run `rand_files.py` on the head node of a
# cluster.
source .miniconda3/etc/profile.d/conda.sh
conda activate cloud-data
cd random-file-generator
python rand_files.py
ssh -q usercontainer "export rand_filenames='${RAND_FILENAMES}'" # Send locations and names of randomly
                                                              # generated files back to user container