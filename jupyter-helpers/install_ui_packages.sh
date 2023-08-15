#!/bin/bash
miniconda_path=$1
conda_env=$2



source ${miniconda_path}/etc/profile.d/conda.sh
conda activate ${conda_env}


if [ -z "$( conda list | grep -w "ipywidgets" | grep -w "8.0.5" )" ]
then
    echo "Installing ipywidgets..."
    conda install -y -c conda-forge ipywidgets=8.0.5
    echo "Done."
fi

if [ -z "$( conda list | grep -w "pandas" )" ]
then
    echo "Installing pandas..."
    conda install -y pandas
    echo "Done."
fi

if [ -z "$( conda list | grep -w "jupyter-ui-poll" )" ]
then
    echo "Installing jupyter-ui-poll..."
    pip install jupyter-ui-poll
    echo "Done."
fi