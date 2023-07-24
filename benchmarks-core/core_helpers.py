"""This script defines a few commonly-used classes within the benchmarking.
These include:

    * DiagnosticTimer
        A class used to time operations and record the data in a Pandas dataframe
    * DevNullStore
        A class that creates a Python dictionary that forgets whatever is put in it.
        Similar to porting files to `/dev/null`
"""


from contextlib import contextmanager
import pandas as pd
import time
import os
import gcsfs
import s3fs
import xarray as xr
import dask
import intake_xarray


def get_storage_options(csp, bucket_type, credentials):
    """Depending on the cloud service provider and
    bucket type, determines what the value of the
    `storage_options` keyword argument should be
    """
    if bucket_type == 'Public':
        storage_options = {'anon': True}
    elif csp == 'GCP' and bucket_type == 'Private':
        storage_options = {'token': credentials}
    elif csp == 'AWS' and bucket_type == 'Private':
        storage_options = {'anon': False, 'profile_name': credentials}
    else:
        storage_options = None

    return storage_options


def SetFileSystem(location : str, csp : str, bucket_type : str, storage_options : dict):
    """Opens a cloud storage filesystem to write to nonmounted locations

    Parameters
    ----------
    location : str
        Cloud object store URI
    csp : str
        Cloud service provider of the bucket. Tells the function which filesystem
        to initialize
    bucket_type : str
        One of two options: Public or Private
    storage_options : dict
        A dictionary containing storage options to pass to a filesystem

    Returns
    -------
    fs : filesystem object
        An object that references the cloud storage filesystem opened
        in the function. Passed back to `write(...)`
    """
    
    if csp == 'GCP' and bucket_type == 'Public':
        fs = gcsfs.GCSFileSystem(location)

    elif csp == 'GCP' and bucket_type == 'Private':
        fs = gcsfs.GCSFileSystem(location, token=storage_options['token'])

    elif csp == 'AWS' and bucket_type == 'Public':
        fs = s3fs.S3FileSystem(location, anon=storage_options['anon'])

    elif csp == 'AWS' and bucket_type == 'Private':
        fs = s3fs.S3FileSystem(location, anon=storage_options['anon'], profile=storage_options['profile_name'])

    return fs


def combine_nc_subfiles(store, file, storage_options, filesystem):

    subfiles = filesystem.ls(f'{base_uri}/{file}')
    uri_prefix = base_uri.split('//')[0]
    datasets = []

    for subfile in subfiles:
        subset = intake_xarray.netcdf.NetCDFSource(f'{uri_prefix}//{subfile}', storage_options=storage_options, chunks={}).to_dask()
        datasets.append(subset)
    ds = xr.combine_by_coords(datasets)
    
    return ds





class DiagnosticTimer:
    """
    This class is used to time a wide range of operations
        in the benchmarking: preprocessing, reading, performing
        mathematical operations, etc. It stores the data 

    Attributes
    ----------
    time_desc:
        A string used to describe what the timing data is measuring

    Methods
    -------
    time():
        Measures the time it take to execute any commands coded in
        after its call

        Sample call:
            with DiagnosticTimer.time(**kwargs):
                <python commands>

    dataframe():
        Creates and returns a Pandas dataframe containing timing 
        data and other keyword arguments specified by the user
    """

    def __init__(self, time_desc='Elapsed Time'):
        """This class is used to time a wide range of operations
        in the benchmarking: preprocessing, reading, performing
        mathematical operations, etc. It stores the data 

        Parameters
        ----------
        time_desc : str (default = 'Elapsed Time')
            Describes which operation is being timed. For example,
            a good title for the time it takes for data to be read
            from the cloud might be `time_desc='
        """
        self.time_desc = time_desc
        self.diagnostics = []
        
    @contextmanager
    def time(self, **kwargs):
        """Records execution time for Python commands
        executed under its call

        Parameters
        ----------
        **kwargs : kwarg
            Pass any number of keyword arguments to be recorded
            in a list
        """
        tic = time.time()
        yield
        toc = time.time()
        kwargs[self.time_desc] = toc - tic
        self.diagnostics.append(kwargs)
        
    def dataframe(self):
        """Populates a Pandas dataframe with keyword arguments
        that include the timing data measured with the `time()`
        attribute

        Returns
        -------
        df : Pandas dataframe
            A Pandas dataframe containing all keyword
            arguments and timing data
        """
        df = pd.DataFrame(self.diagnostics)
        return df


class DevNullStore:
    """
    A class that creates a Python
    dictionary that forgets whatever
    is put into it
    """
    def __init__(self):
        pass
    def __setitem__(*args, **kwargs):
        pass
