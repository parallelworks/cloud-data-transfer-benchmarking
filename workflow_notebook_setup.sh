#!/bin/bash
################DESCRIPTION####################
# Runs notebook setup for cloud data transfer
# benchmarking workflow. Setup includes miniconda
# installation and required environment build on
# resources requested by the user

# Python installation call
bash $( pwd )/utils/install_python.sh ${resources}

# Random Number Generation:
# First check if user has specified any files to randomly generate
if [ -n "${randgen_files}" ]
then
    # Loop random generation file creation for each specified resource
    for resource in ${resources}
    do
        # Define environment variables on the remote machine
        # and run random file generation python script
        ssh ${resource}.clusters.pw "RANDGEN_FILES=\"{randgen_files}\" \
                                    RANDGEN_SIZES=\"{randgen_sizes}\" \
                                    python rand_files.py"
    done
fi