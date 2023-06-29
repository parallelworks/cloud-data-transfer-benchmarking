#!/bin/bash
#                 SCRIPT DESCRIPTION
#====================================================
# Download Miniconda, install,
# and create a new environment
# on specified cloud resources.

#
# For moving conda envs around,
# it is possible to put the
# miniconda directory in a tarball
# but the paths will need to be
# adjusted.  The download and
# decompression time can be long.
# As an alternative, consider:
# conda list -e > requirements.txt
# to export a list of the req's
# and then:
# conda create --name <env> --file requirements.txt
# to build another env elsewhere.
# This second step runs faster
# than this script because
# Conda does not stop to solve
# the environment.  Rather, it
# just pulls all the listed
# packages assuming everything
# is compatible.
#====================================================

#                                     SCRIPT INPUTS
#======================================================================================
# install_python.sh '["<resource-1>","<resource-2>",...,"<resource-n>"]'
#
# Above is a sample call to this script. The only inputs given to this script should
# be the name of the resources that conda and the corresponding environment will be
# installed on. If run from workflow form or Jupyter notebook, these variables will
# be passed into the script automatically. If testing the script on the command line,
# these inputs will have to be passed in manually.
#======================================================================================

#                    USER VARIABLE CHOICES
#=============================================================
# Miniconda installation directory.
# `source` command does not work with "~", 
# so put an absolute path on the line below
miniconda_dir="${HOME}/.miniconda3"
conda_version="latest" # Change for desired miniconda version
conda_env="cloud-data" # Desired name of conda environment
#=============================================================

#                                       FUNCTION DEFINITIONS
#==================================================================================================
# 1. MINICONDA INSTALLATION
f_install_miniconda() {
    install_dir=$1
    conda_version=$2
    if [ -e "${install_dir}/etc/profile.d/conda.sh" ]
    then
        echo "Miniconda is already installed in \"${install_dir}\"!"
    else
        conda_repo="https://repo.anaconda.com/miniconda/Miniconda3-${conda_version}-Linux-x86_64.sh"
        ID=$(date +%s)-${RANDOM} # This script may run at the same time!
        nohup wget ${conda_repo} -O /tmp/miniconda-${ID}.sh 2>&1 > /tmp/miniconda_wget-${ID}.out
        rm -rf ${install_dir}
        mkdir -p $(dirname ${install_dir})
        nohup bash /tmp/miniconda-${ID}.sh -b -p ${install_dir} 2>&1 > /tmp/miniconda_sh-${ID}.out
        rm /tmp/miniconda-${ID}.sh
    fi
}

# 2. CONDA ENVIRONMENT INSTALLATION
f_install_env() {
    my_env=$1
    miniconda_dir=$2
    localpath=$3
    env_filename="${my_env}_requirements.yml"
    python_version="" # Choose specific python version or leave blank

    # Start conda and activate base environment
    source ${miniconda_dir}/etc/profile.d/conda.sh
    conda activate base

    # Install packages for hosting Jupyter notebooks
    # NOTE: May not need these for cloud-data environments since
    # the benchmarking isn't running on Jupyter servers hosted in
    # the head nodes of clusters
    #conda install -y ipython
    #conda install -y -c conda-forge jupyter
    #conda install -y nb_conda_kernels
    #conda install -y -c anaconda jinja2
    #conda install -y requests
    #conda install nbconvert
    #pip install remote_ikernel

    # Check if the environment of the same name is already built in conda
    env_check=$( conda env list | grep -w ${my_env} | cut -d ' ' -f 1 )
    if [ "${env_check}" == "${my_env}" ] && [ -e ${env_filename} ]
    then
        echo "Updating exisiting environment \"${my_env}\"..."
        conda env update -n ${my_env} -f ${env_filename}
        echo "Environment updated."
        rm ${env_filename} # Clean up
    elif [ -e ${env_filename} ]
    then
        conda env create -f ${env_filename}
        # Will build environment from requirements file
        # if it exists
        rm ${env_filename} # Clean up
    elif [[ ${my_env} == "base" ]]
    then
        echo "Done installing packages in base environment."
    else
        # We often run Jupter notebooks so include ipython here.
        conda create -y --name ${my_env} python${python_version} ipython

        # Jump into new environment
        conda activate ${my_env}

        # Install packages for connecting kernels to notebooks in base
        conda install -y requests
        conda install -y ipykernel
        conda install -y -c anaconda jinja2

        # Other more specialized packages.
        # Can be edited to desired evironment.
        conda install -y -c conda-forge dask
        conda install -y -c conda-forge h5netcdf
        conda install -y -c conda-forge xarray
        conda install -y -c conda-forge netCDF4
        conda install -y -c conda-forge bottleneck
        conda install -y -c conda-forge intake-xarray
        conda install -y -c conda-forge matplotlib
        conda install -y -c conda-forge gcsfs
        conda install -y -c conda-forge s3fs
        conda install -y -c conda-forge fastparquet
        pip install pyarrow
        pip install scipy

        # Write out the ${my_env}_requirements.yml to document environment
        conda env export > ${env_filename}
        scp -q ${env_filename} usercontainer:${localpath}
        rm ${env_filename}
    fi
}
#==================================================================================================

#                                            EXECUTE INSTALLATION
#=============================================================================================================
# Loop executes conda installation and environment construction for each resource specificed in script inputs.
local_wd=$(pwd)/utils

for resource in $(echo "$1" | jq -r '.[]')
do
    miniconda_dir_ref=${miniconda_dir}
    echo "Will install to \"${miniconda_dir_ref}\""
    # Install miniconda
    echo -e "Installing Miniconda-${conda_version} on \"${resource}\"...\n"
    ssh ${resource}.clusters.pw "$(typeset -f f_install_miniconda); \
                                 f_install_miniconda ${miniconda_dir_ref} ${conda_version}"
    echo -e "\nFinished installing Miniconda on \"${resource}\"."

    # Checks to see if local copy of requirements file exists.
    # If so, copies over to the current remote resource in the loop.
    if [ -e "${local_wd}/${conda_env}_requirements.yml" ]
    then
        scp -q ${local_wd}/${conda_env}_requirements.yml ${resource}.clusters.pw:
    fi

    # Build environment
    echo -e "Building \"${conda_env}\" environment on \"${resource}\"...\n"
    ssh ${resource}.clusters.pw "$(typeset -f f_install_env); \
                                 f_install_env ${conda_env} ${miniconda_dir_ref} ${local_wd}"
    echo -e "\nFinished building \"${conda_env}\" environment on \"${resource}\"."
done

echo -e "\nDone installing Miniconda-${conda_version} and building \`${conda_env}\` on all requested resources."