#!/bin/bash
################DESCRIPTION####################
# Runs notebook setup for cloud data transfer
# benchmarking workflow. Setup includes miniconda
# installation and required environment build on
# resources requested by the user

# Python installation call
bash $( pwd )/utils/install_python.sh $@

# Random Number Generation
python rand_files.py