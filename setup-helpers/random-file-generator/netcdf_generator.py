import os
import xarray as xr
import dask.array as da
import pandas as pd
import numpy as np
import itertools
import fsspec


def compute_dim_length(filesize : float, nc_info : dict) -> int:
    """Compute the dimensions of the NetCDF axes based on the requested
    number of axes and filesize

    Parameters
    ----------
    filesize : float
        Desired size of the NetCDF4 file (in GB)
    nc_info : dict
        A dictionary containing user inputs concerning the filesize,
        number of axes, and number of data variables the desired
        NetCDF4 file should be defined by

    Returns
    -------
    dim_length : int
        Length that all axes will be set to (i.e., all axes specifed by the user
        will have this number of elements)
    """

    dvars = nc_info['Data Variables']
    float_axes = nc_info['Float Coords']
    time_axes = 1

    # Define constants used for finding dimension lengths
    GB2Byte = 1000000000 # Conversion factor from GB to B
    value_bytes = 8 # four bytes for each float value in the data variable
    coord_bytes = 8 # four bytes for each integer coordinate value
    time_bytes = 8 # eight bytes for each time value
    overhead = 0 # Will have to compute seperately
    n_dims = float_axes + time_axes # Total number of desired dimensions


    # Populate polynomial coefficients list
    coeffs = [dvars*value_bytes] # Number of data variables
    for i in range(int(n_dims)-2):
        coeffs.append(0) # All coefficents of polynomial terms with
                            # order n-1 to 2 will be zero
    coeffs.append(coord_bytes*float_axes + time_bytes*time_axes) # Dimension coefficient
    coeffs.append(-filesize*GB2Byte + overhead) # Final term representing filesize & overhead


    # Find only positive roots and determine
    # the integer representing coordinate length
    sols = np.roots(coeffs)
    for i in sols:
        if i.imag == 0 and i.real > 0:
            dim_length = round(i.real)

    return dim_length




def define_dataset(nc_info : dict, dim_length : int):
    """Setup the xarray dataset that will be used to write the randomly-generated
    NetCDF4 file
    
    Parameters
    ----------
    nc_info : dict
        A dictionary containing user inputs concerning the filesize,
        number of axes, and number of data variables the desired
        NetCDF4 file should be defined by
    dim_length : int
        The dimension length computed from `compute_dim_length(...)`

    Returns
    -------
    ds : Xarray dataset object
        Dataset structured as a NetCDF. Is not loaded into memory
        to allow for out-of-memory remote file writes
    """

    # Load user input for axes and data variablees
    dvars = nc_info['Data Variables']
    float_axes = nc_info['Float Coords']
    time_axes = 1

    # Create axes
    axs_names = []
    axs_info = []

    # Float axes
    for i in range(int(float_axes)):
        axs_names.append('f' + str(i+1))
        axs_info.append(list(np.linspace(0, dim_length/2, dim_length)))

    # Time axes
    for i in range(int(time_axes)):
        axs_names.append('t' + str(i+1))
        axs_info.append(pd.date_range("2023-01-01", periods=dim_length))

    # Store names and coordinates
    axs_names = tuple(axs_names)
    coords = dict(zip(axs_names, axs_info))


    # Data variables
    shape = tuple([dim_length] * int(float_axes + time_axes)) # Shape of data variable arrays
    var_names = []
    data_info = []
    for i in range(int(dvars)):
        var_names.append('random' + str(i+1))
        data_info.append((axs_names, da.random.random(shape)))

    # Store data variables
    data_vars = dict(zip(var_names, data_info))
    
    ds = xr.Dataset(data_vars = data_vars, coords = coords)
    ds = ds.chunk(chunks = "auto")

    return ds



def split_by_chunks(dataset):
    "Splits the defined dataset into sub-datasets"

    chunk_slices = {}
    for dim, chunks in dataset.chunks.items():
        slices = []
        start = 0
        for chunk in chunks:
            if start >= dataset.sizes[dim]:
                break
            stop = start + chunk
            slices.append(slice(start, stop))
            start = stop
        chunk_slices[dim] = slices
    for slices in itertools.product(*chunk_slices.values()):
        selection = dict(zip(chunk_slices.keys(), slices))
        yield dataset[selection]


def create_filepath(root_path, counter):
    "Create unique filenames for each chunk of a NetCDF4 file"
    
    filepath = f'{root_path}chunk{counter}.nc'
    return filepath



def write(filesize : float, storage_info : dict, nc_info : dict) -> str:
    """Generates NetCDF4 file of a given size. The generated NetCDF4 file
    may have any number of dimensions that a user wishes. Currently, this
    works by first making a local copy and then uploading to cloud storage.
    While direct upload to cloud storage is supported in serial uploads,
    it is not while using Dask to write in parallel. We use parallel
    functionality because, often, randomly generated data will be out-of-memory
    and impossible to create unless done in parallel.

    Parameters
    ----------
    filesize : float
        Desired size of the randomly generated NetCDF4 file (in GB)
    storage_info : dict
        A dictionary containing information about all storage locations
        that randomly-generated files are to be written to.
    nc_info : dict
        Contains information about the desired number of data variables and axes to use

    Returns
    -------
    full_path : str        
        Full path of randomly generated file written into a provided cloud
        storage location. This information is passed back to the main workflow
        for later use, and will represent either a full URI of the object location
        or POSIX filesystem path corresponding to the locaton of the object if the
        storage is mounted
    """

    # Get dimension length and define dataset
    dim_length = compute_dim_length(filesize, nc_info)
    ds = define_dataset(nc_info, dim_length)

    # Split dataset into seperate datasets based on chunksize
    datasets = list(split_by_chunks(ds))

    # Choose root directory. Since Xarray does not yet support
    # direct writes to remote cloud storage, if the storage is
    # not mounted it must be first created locally and copied
    # to cloud storage.
    filename = 'random_' + str(filesize) + 'GB_NetCDF4/'
    local_root = './' + filename


    # Make an empty directory
    os.system(f'mkdir -p {local_root}')

    # Assign filenames to each NetCDF4 subfile
    paths = []
    for i in range(len(datasets)):
        if len(str(i)) == 1:
            counter = f'0{i}'
        else:
            counter = str(i)

        paths.append(create_filepath(local_root, counter))
        # Make empty files with the created filepaths
        os.system(f'touch {paths[i]}')

    # Create local copy of dataset in parallel
    xr.save_mfdataset(datasets=datasets, paths=paths)


    # Loop through all storage locations and copy to cloud storage
    for n in range(len(storage_info)):

        # Grab info about current cloud storage location
        current_uri = storage_info[n]['Path']
        csp = storage_info[n]['CSP']
        bucket_type = storage_info[n]['Type']

        home = os.path.expanduser('~')
        key_dir = f'{home}/cloud-data-transfer-benchmarking/storage-keys'

        if csp == 'GCP' and bucket_type == 'Private':
            tmp_crds = storage_info[n]['Credentials']['token'].split('/')[-1]
            storage_info[n]['Credentials']['token'] = f'{key_dir}/{tmp_crds}'

        storage_options = storage_info[n]['Credentials']
        remote_root = f'{current_uri}/cloud-data-transfer-benchmarking/randfiles/{filename}'

        # Copy local files to cloud storage if the bucket is not mounted
        if bucket_type != 'PW Mounted':
            fs = fsspec.filesystem(current_uri.split(':')[0], **storage_options)
            fs.put(local_root, remote_root, recursive=True) # This needs to eventually be a multi-threaded copy


        #Confirm successful write
        print(f'Files written to \"{remote_root}\"')
    
    # Remove local files
    os.system(f'rm -r {local_root}')

    # Return the name of the randomly generated file
    return f'{filename}*'