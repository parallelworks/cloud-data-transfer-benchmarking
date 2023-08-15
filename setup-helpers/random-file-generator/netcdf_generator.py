import os
import xarray as xr
import dask.array as da
import pandas as pd
import numpy as np
import itertools
import fsspec
import copy
from h5netcdf.legacyapi import Dataset


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

    # Define constants used for finding dimension lengths
    GB2Byte = 1000000000 # Conversion factor from GB to B
    value_bytes = 8 # eight bytes for each float value in the data variable
    coord_bytes = 8 # eight bytes for each integer coordinate value
    overhead = 0 # Will have to compute seperately
    n_dims = float_axes # Total number of desired dimensions


    # Populate polynomial coefficients list
    coeffs = [dvars*value_bytes] # Number of data variables
    for i in range(int(n_dims)-2):
        coeffs.append(0) # All coefficents of polynomial terms with
                            # order n-1 to 2 will be zero
    coeffs.append(coord_bytes*float_axes) # Dimension coefficient
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

    # Create axes
    axs_names = []
    axs_info = []

    # Float axes
    for i in range(int(float_axes)):
        axs_names.append('f' + str(i+1))
        axs_info.append(list(np.linspace(0, dim_length/2, dim_length)))

    # Store names and coordinates
    axs_names = tuple(axs_names)
    coords = dict(zip(axs_names, axs_info))


    # Data variables
    shape = tuple([dim_length] * int(float_axes)) # Shape of data variable arrays
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


class ncfile_generator:
    def __init__(self, nc_path):
        self.nc_path = nc_path

    def open_ncfile(self):

        try: self.ncfile.close()
        except: pass
        self.ncfile = Dataset(self.nc_path, mode='w')

    def close_ncfile(self):
        self.ncfile.close()


    def create_dimension(self, dimname='dim', length=None):
        return self.ncfile.createDimension(dimname, length)


    def create_datavar(self, varname='var', dtype=np.float64, dimensions=(), chunksizes=()):
        return self.ncfile.createVariable(varname, dtype, dimensions, chunksizes=chunksizes)


    def split_by_chunks(self, dataset, datavar, nc_var):
        "Splits the defined dataset into sub-datasets"
        dataarray = dataset[datavar]

        chunk_slices = {}
        for dim, chunks in dataarray.chunksizes.items():
            slices = []
            start = 0
            for chunk in chunks:
                if start >= dataarray.sizes[dim]:
                    break
                stop = start + chunk
                slices.append(slice(start, stop))
                start = stop
            chunk_slices[dim] = slices

        print(f'Writing data variable \"{datavar}\"...')
        for slices in itertools.product(*chunk_slices.values()):
            selection = dict(zip(chunk_slices.keys(), slices))

            nc_var[slices] = dataarray[selection].values

        print(f'Data variable \"{datavar}\" written to \"{self.nc_path}\"')
            







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
    """

    # Get dimension length and define dataset
    dim_length = compute_dim_length(filesize, nc_info)
    ds = define_dataset(nc_info, dim_length)

    # Choose root directory. Since Xarray does not yet support
    # direct writes to remote cloud storage, if the storage is
    # not mounted it must be first created locally and copied
    # to cloud storage.
    filename = 'random_' + str(filesize) + 'GB_NetCDF4.nc'
    local_root = './' + filename
    os.system(f'touch {local_root}')


    # Create dimensions, define coordinate axes, and write dimension data
    nc_gen = ncfile_generator(local_root)
    nc_gen.open_ncfile()
    dims = []
    dim_vars = []
    for dim, length in ds.dims.items():
        dims.append(nc_gen.create_dimension(dimname=dim, length=length))
        
        dim_vars.append(nc_gen.create_datavar(varname=dim, dimensions=(dim,), chunksizes=(int(max(ds.chunks[dim])),)))
        dim_vars[-1][:] = ds.coords[dim].values

    # Create data vars
    xr_data_vars = [var for var in ds.data_vars]
    data_vars = []
    for var in xr_data_vars:
        chunksizes = tuple([max(value) for key, value in ds[var].chunksizes.items()])
        dim_names = tuple([dim for dim in ds[var].dims])
        data_vars.append(nc_gen.create_datavar(varname=var, dimensions=dim_names, chunksizes=chunksizes))

        # Write data variable data
        nc_gen.split_by_chunks(ds, var, data_vars[-1])

    nc_gen.close_ncfile() # Close and save all data


    # Loop through all storage locations and copy to cloud storage
    for n in range(len(storage_info)):

        # Grab info about current cloud storage location
        current_uri = storage_info[n]['Path']
        csp = storage_info[n]['CSP']
        bucket_type = storage_info[n]['Type']

        home = os.path.expanduser('~')
        key_dir = f'{home}/cloud-data-transfer-benchmarking/storage-keys'

        if csp == 'GCP' and bucket_type == 'Private':
            storage_options = copy.copy(storage_info[n]['Credentials'])
            tmp_crds = storage_options['token'].split('/')[-1]
            storage_options['token'] = f'{key_dir}/{tmp_crds}'
        else:
            storage_options = storage_info[n]['Credentials']
            
        remote_root = f'{current_uri}/cloud-data-transfer-benchmarking/randfiles/{filename}'

        # Copy local files to cloud storage if the bucket is not mounted
        if bucket_type != 'PW Mounted':
            fs = fsspec.filesystem(current_uri.split(':')[0], **storage_options)
            print(f'Uploading \"{local_root}\" to \"{remote_root}\"...')
            fs.put_file(local_root, remote_root) # This needs to eventually be a multi-threaded copy


        #Confirm successful write
        print(f'File written to \"{remote_root}\"')