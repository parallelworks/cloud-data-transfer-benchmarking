#!/bin/bash

# This script simply orchestrates the necessary steps
# need to run `rand_files.py` on the head node of a
# cluster.
function finish {
    echo "Shredding run_rand_files.sh"; shred -u run_rand_files.sh;
}

source .miniconda3/etc/profile.d/conda.sh
conda activate cloud-data
python rand_files.py
rm rand_files.py

trap finish EXIT