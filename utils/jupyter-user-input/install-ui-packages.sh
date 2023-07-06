#!/bin/bash

source ${HOME}/.miniconda3c/etc/profile.d/conda.sh
conda activate jupyter

if [ -z "$( conda list | grep -w "ipywidgets" | grep -w "8.0.5" )" ]
then
    echo "Installing ipywidgets..."
    conda install -y -c conda-forge ipywidgets==8.0.5
    echo "Done"
fi

if [ -z "$( conda list | grep -w "jupyter-ui-poll" )" ]
then
    echo "Installing jupyter-ui-poll..."
    pip install jupyter-ui-poll
    echo "Done"
fi