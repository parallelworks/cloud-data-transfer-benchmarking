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
# 1. MINICONDA INSTALLATION AND "cloud-data"
#    ENVIRONMENT CONSTRUCTON:
#bash $( pwd )/utils/install_python.sh ${resources}

# 2. RANDOM NUMER FILE GENERATION:
# |              Future Improvements               |
# | Conditional statements that check if there are |
# | multiple cloud storage locations that the user |
# | would like to write random data to. In the     |
# | that there are, check for a cluster of the     |
# | same CSP as the storage and write using that.  |

# First check if user has specified any files to randomly generate
if [ -n "${randgen_files}" ]
then
    # Pass requried environment variables to remote head node
    # and run random file generation creation script
    resource=$( echo "${resources}" | jq -r '.[0]' )
    scp -q $( pwd )/utils/rand_files.py ${resource}.clusters.pw: 
    ssh ${resource}.clusters.pw "RANDGEN_FILES=\"${randgen_files}\" \
                                RANDGEN_SIZES=\"${randgen_sizes}\" \
                                RANDGEN_STORES=\"${benchmark_storage}\" \
                                source .miniconda3/etc/profile.d/conda.sh \
                                conda activate cloud-data \
                                python rand_files.py"
else
    echo No randomly generated files will be created.
fi