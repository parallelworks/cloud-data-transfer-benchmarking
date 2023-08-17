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
# install_python.sh "/path/to/input/file"
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
conda_version="23.5.2" # Change for desired miniconda version
conda_env="cloud-data" # Desired name of conda environment
#=============================================================

#                    INPUTS FROM WORKFLOW
#==============================================================
input_file=$1
resources=$( jq -r '.RESOURCES[] | .SSH' ${input_file} )

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
        conda_repo="https://repo.anaconda.com/miniconda/Miniconda3-py311_${conda_version}-0-Linux-x86_64.sh"
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
    env_filename="${my_env}-requirements.yaml"
    python_version="3.11.4" # Choose specific python version or leave blank

    # Start conda and activate base environment
    source ${miniconda_dir}/etc/profile.d/conda.sh

    # Check if the environment of the same name is already built in conda
    env_check=$( conda env list | grep -w ${my_env} | cut -d ' ' -f 1 )
    update_env=0 # This line should be set to 1 if you wish to update
                 # the conda environment, and 0 if you want to simply
                 # declare that the environment exists and move on

    if [ "${env_check}" == "${my_env}" ] && [ -e ${env_filename} ]
    then
        if [ ${update_env} -eq 1 ]
        then
            echo "Updating exisiting environment \"${my_env}\"..."
            conda env update -n ${my_env} -f ${env_filename}
            echo "Environment updated."
        else
            echo "Environment already exists!"
        fi
        rm ${env_filename} # Clean up

    elif [ -e ${env_filename} ]
    then
        echo "Creating environment from \"${env_filename}\""
        conda env create -f ${env_filename}
        # Will build environment from requirements file
        # if it exists
        rm ${env_filename} # Clean up
        
    else
        # We often run Jupter notebooks so include ipython here.
        conda create -y --name ${my_env} python=${python_version}

        # Jump into new environment
        conda activate ${my_env}

        # Other more specialized packages.
        # Can be edited to desired evironment.

        # Dask
        conda install -y dask -c conda-forge
        conda install -y dask-jobqueue -c conda-forge

        # Xarray
        conda install -y -c conda-forge xarray
        conda install -y -c conda-forge bottleneck
        conda install -y -c conda-forge intake-xarray
        conda install -y -c conda-forge fastparquet
        conda install -y h5netcdf

        # Remote filesystems
        conda install -y -c conda-forge fsspec
        conda install -y -c conda-forge gcsfs
        conda install -y -c conda-forge s3fs

        # Add this back in if virtual dataset functionality
        # in `benchmarks-core/core_helpers.py` is fixed
        #conda install -y -c conda-forge kerchunk

        # Other
        conda install -y -c anaconda ujson
        conda install -y -c conda-forge numcodecs

        # Pip dependencies
        #pip install netCDF4
        #pip install pyarrow
        pip install scipy
        pip install google-auth-oauthlib==1.0.0
        pip install msgpack==1.0.5

        # Write out the ${my_env}-requirements.yaml to document environment
        conda env export | grep -v "^prefix: " > ${env_filename}
        scp -q ${env_filename} usercontainer:${localpath}
        rm ${env_filename}
    fi
}
#==================================================================================================

#                                            EXECUTE INSTALLATION
#=============================================================================================================
# Loop executes conda installation and environment construction for each resource specificed in script inputs.
local_wd=$( pwd )/setup-helpers/python-installation

resource_index=0
for resource in ${resources}
do
    resource_name=$( jq -r ".RESOURCES[${resource_index}] | .Name" ${input_file} )

    # Determine if user has specified a miniconda installation directory
    miniconda_dir_ref=$( jq -r ".RESOURCES[${resource_index}] | .MinicondaDir" ${input_file} )
    if [ "${miniconda_dir_ref}" == "~" ]
    then
        miniconda_dir_ref="${HOME}/.miniconda3"
    fi


    echo "Will install miniconda3 to \"${miniconda_dir_ref}\""
    # Install miniconda
    echo -e "Installing Miniconda-${conda_version} on \"${resource_name}\"..."
    ssh -q -o StrictHostKeyChecking=no ${resource} "$(typeset -f f_install_miniconda); \
                                                    f_install_miniconda ${miniconda_dir_ref} ${conda_version}"
    echo -e "Finished installing Miniconda on \"${resource_name}\".\n"

    # Checks to see if local copy of requirements file exists.
    # If so, copies over to the current remote resource in the loop.
    if [ -e "${local_wd}/${conda_env}-requirements.yaml" ]
    then
        scp -q -o StrictHostKeyChecking=no ${local_wd}/${conda_env}-requirements.yaml ${resource}:
    fi

    # Build environment
    echo -e "Building \"${conda_env}\" environment on \"${resource_name}\"..."
    ssh -q -o StrictHostKeyChecking=no ${resource} "$(typeset -f f_install_env); \
                                                    f_install_env ${conda_env} ${miniconda_dir_ref} ${local_wd}"
    echo -e "Finished building \"${conda_env}\" environment on \"${resource_name}\".\n"

    let resource_index++
done

echo -e "Done installing Miniconda-${conda_version} and building \`${conda_env}\` on all requested resources.\n\n"