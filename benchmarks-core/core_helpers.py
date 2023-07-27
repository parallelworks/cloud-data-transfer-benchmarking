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


def check_for_userpath(check_var, base_uri):
    """Check if the input variable is a single filepath
    or a list of filepaths from `file_list.json`. If the
    input is a single filepath,simply return that path.

    If the filepath is a list, there will only be two
    possible paths: the original location of the data,
    and the location that the data was transferred to.
    If the base URI of the original location is the same
    as the base uri of the input, then this function returns
    the original filepath. Otherwise, the more general path
    is taken.

    Parameters
    ----------
    check_var : str OR list
        A string of a filepath or list of two different
        strings of filepaths.
    base_uri : str
        A base URI of a cloud storage location (e.g.,
        "gs://my-bucket", "s3://my-bucket)

    Returns
    -------
    str
        A filepath

    Examples
    --------
    1)
        Inputs
        ------
        - check_var = ["gs://cloud-data-benchmarks/path/to/data.csv",
                       "cloud-data-transfer-benchmarking/userfiles/data.csv"]
        - base_uri = "gs://cloud-data-benchmarks

        Output
        ------
        "path/to/data.csv"

    2)
        Inputs
        ------
        - check_var = (Same as example 1)
        - base_uri = "s3://cloud-data-benchmarks

        Output
        ------
        "cloud-data-transfer-benchmarking/userfiles/data.csv"

    3)
        Inputs
        ------
        - check_var = "cloud-data-transfer-benchmarking/userfiles/data.csv"
        - base_uri = "gs://cloud-data-benchmarks"

        Output
        ------
        "cloud-data-transfer-benchmarking/userfiles/data.csv
    
    4)
        Inputs
        ------
        - check_var = ["gs://cloud-data-benchmarks/path/to/data.csv",
                       "cloud-data-transfer-benchmarking/userfiles/data.csv",
                       "s3://cloud-data-benchmarks/path/to/data.csv"]
        - base_uri = "s3://cloud-data-benchmarks"

        Output
        ------
        "/path/to/data.csv
    """

    if type(check_var) == str:
        return check_var
    elif type(check_var) == list:
        matching_uri = [ele[(len(base_uri)+1):] for ele in check_var if ele[:len(base_uri)] == base_uri]
        if any(matching_uri):
            return matching_uri[0]
        else:
            return check_var[-1]




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
        storage_options = {'anon': False, 'profile': credentials}
    else:
        storage_options = None

    return storage_options


def SetFileSystem(csp : str, bucket_type : str, storage_options : dict):
    """Opens a cloud storage filesystem to write to nonmounted locations

    Parameters
    ----------
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
        fs = gcsfs.GCSFileSystem()

    elif csp == 'GCP' and bucket_type == 'Private':
        fs = gcsfs.GCSFileSystem(token=storage_options['token'])

    elif csp == 'AWS' and bucket_type == 'Public':
        fs = s3fs.S3FileSystem(anon=storage_options['anon'])

    elif csp == 'AWS' and bucket_type == 'Private':
        fs = s3fs.S3FileSystem(anon=storage_options['anon'], profile=storage_options['profile'])

    return fs


def combine_nc_subfiles(base_uri, file, storage_options, filesystem):
    subfiles = filesystem.ls(f'{base_uri}/{file[:-1]}')
    uri_prefix = base_uri.split('//')[0]
    datasets = []

    for subfile in subfiles:
        subset = intake_xarray.netcdf.NetCDFSource(f'{uri_prefix}//{subfile}', storage_options=storage_options, chunks={}).to_dask()
        yield subset





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