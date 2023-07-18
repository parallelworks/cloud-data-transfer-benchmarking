import os
import xarray as xr
import dask.array as da
import pandas as pd
import numpy as np
import itertools
import gcsfs


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
    time_axes = nc_info['Time Coords']

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
    time_axes = nc_info['Time Coords']

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
    
    filepath = f'{root_path}chunk{str(counter)}.nc'
    return filepath


def SetFileSystem(location : str, csp : str, token : str):
    """Opens a cloud storage filesystem to write to nonmounted locations

    Parameters
    ----------
    location : str
        Cloud object store URI
    csp : str
        Cloud service provider of the bucket. Tells the function which filesystem
        to initialize
    token : str
        Local path to the cloud storage access token. This will be located within
        the randomly-generated file options folder of the cluster's head node

    Returns
    -------
    fs : filesystem object
        An object that references the cloud storage filesystem opened
        in the function. Passed back to `write(...)`
    """
    
    # TODO: Add S3 filesystem support
    match csp:
        case 'GCP':
            fs = gcsfs.GCSFileSystem(location, token=token)
    return fs


def choose_root(local_root : str, remote_root : str, bucket_type : str):
    "Determines which root path to use to write files"

    match bucket_type:
        case 'PW Mounted':
            root = remote_root
        case other:
            root = local_root
    
    return root



def write(filesize : float,
        location : str,
        bucket_type : str,
        csp : str,
        token : str,
        nc_info : dict) -> str:
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
    location : str
        Cloud object store URI or local path to mounted filesystem
    bucket_type : str
        One of three options given from user input in `main.ipynb`:
        Public, Private, or PW Mounted
    csp : str
        Cloud service provider of the bucket
    token : str
        Local path to the cloud storage access token. This will be located within
        the randomly-generated file options folder of the cluster's head node
    nc_info : dict
        A dictionary containing information about the desired number of data
        variables and axes to write the NetCDF4 file with

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
    remote_root = location + filename
    root = choose_root(local_root, remote_root, bucket_type)

    # Make an empty directory
    os.system(f'mkdir {root}')

    # Assign filenames to each NetCDF4 subfile
    paths = []
    for i in range(len(datasets)):
        paths.append(create_filepath(root, (i+1)))
        # Make empty files with the created filepaths
        os.system(f'touch {paths[i]}')

    # Create local copy of dataset in parallel
    xr.save_mfdataset(datasets=datasets, paths=paths)

    # Copy local files to cloud storage if the bucket is not mounted
    if bucket_type != 'PW Mounted':
        fs = SetFileSystem(location, csp, token)
        # TODO: Change next line to parallel upload
        fs.put(root, remote_root, recursive=True)
        os.system(f'rm -r {root}') # Remove local copies of files


    #Confirm successful write and return path of file
    print(f'Files written to \"{remote_root}\"')
    return filename