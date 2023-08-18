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
import xarray as xr
import dask
import intake_xarray
import numpy as np
from scipy.stats import tstd
# from kerchunk.combine import MultiZarrToZarr
# from kerchunk.hdf import SingleHdf5ToZarr
import fsspec
import ujson
import glob
from tempfile import TemporaryDirectory
import copy
import intake



def get_dataset_name(file):
    "Function that gets a dataset name from the filepath"
    split_path = [ele for ele in file.split('/') if len(ele)!=0]
    filename = split_path[-1]
    match filename:
        case '*':
            return split_path[-2]
        case other:
            return filename


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
                       "s3://cloud-data-benchmarks/other/path/to/data.csv"]
        - base_uri = "s3://cloud-data-benchmarks"

        Output
        ------
        "other/path/to/data.csv
    """

    if type(check_var) == str:
        return check_var
    elif type(check_var) == list:
        matching_uri = [ele[(len(base_uri)+1):] for ele in check_var if ele[:len(base_uri)] == base_uri]
        if any(matching_uri):
            return matching_uri[0]
        else:
            return check_var[-1]





# TODO: Figure out how to make virtual datasets work
# class virtual_dataset:

#     def __init__(self, base_uri, file, storage_options, fs):
#         self.base_uri = base_uri
#         self.file = file
#         self.storage_options = storage_options
#         self.fs = fs
#         self.benchmark_dir = os.path.expanduser('~') + '/cloud-data-transfer-benchmarking'
#         self.local_reference = f"{self.benchmark_dir}/references/" + self.file.split('/')[-2] + ".json"
#         self.remote_reference = f"{self.base_uri}/cloud-data-transfer-benchmarking/references/" + self.file.split('/')[-2] + ".json"

#     def generate(self):
#         """Combines datasets that are made up of many NetCDF subfiles
#         and writes them into a Kerchunk reference file.
        
#         This reference file will be stored in all benchmarking cloud
#         storage locations so that chunked NetCDF files can be read 
#         with a single call to `xarray.open_dataset(...)`.
#         """

#         # Gather files from globstring and the URI prefix to each
#         file_paths = self.fs.glob(f'{self.base_uri}/{self.file}')
#         uri_prefix = self.base_uri.split(':')[0]
#         subfiles = sorted([f'{uri_prefix}://{file_path}' for file_path in file_paths])


#         # Check to see which dimensions need to be concatenated
#         ds_chunk1 = intake_xarray.netcdf.NetCDFSource(subfiles[0], storage_options=self.storage_options).to_dask()
#         ds_chunk2 = intake_xarray.netcdf.NetCDFSource(subfiles[1], storage_options=self.storage_options).to_dask()
        

#         def get_coords(ds):
#             "Gets slices of coordinates in an Xarray dataset"
#             data_var = [v for v in ds.data_vars][0]
#             coords = ds[data_var].dims
#             coord_slices = {}
#             for coord in coords:
#                 coord_slices[coord] = ds[data_var].coords[coord].values[0:5]
#             return coord_slices

#         def coord_type(coords_slice1, coords_slice2):
#             """Determines which coordinates between two files
#             in the same dataset have idential coordinates, and
#             which ones will need to be concatenated when creating
#             the virtual dataset.
#             """
#             coord_types = {'concat':[], 'identical':[]}
#             for dim in coords_slice1:
#                 if np.array_equal(coords_slice1[dim], coords_slice2[dim]):
#                     coord_types['identical'].append(dim)
#                 else:
#                     coord_types['concat'].append(dim)
#             return coord_types


#         # Get coordinate slices and determine the type of dimensions in
#         # the dataset (see `coord_type` for more information)
#         chunk1_coords = get_coords(ds_chunk1)
#         chunk2_coords = get_coords(ds_chunk2)
#         coord_types = coord_type(chunk1_coords, chunk2_coords)


#         # Create and update the copy of the storage options dictionary with
#         # other keyword arguments and create a temp directory to 
#         # store reference .json files
#         so = copy.copy(self.storage_options)
#         so.update(dict(mode='rb', default_fill_cache=False, default_cache_type="first"))
#         td = TemporaryDirectory(dir="/mnt/shared")
#         temp_dir = td.name


#         # Function to create a Kerchunk index from a NetCDF subfile
#         def generate_json_ref(file, output_dir, fs, storage_options):
#             with fs.open(file, **storage_options) as infile:
#                 ncchunks = SingleHdf5ToZarr(infile, file)
#                 fname = file.split('/')[-1].rstrip(".nc")
#                 output = f"{output_dir}/{fname}.json"
#                 with open(output, 'wb') as outfile:
#                     outfile.write(ujson.dumps(ncchunks.translate()).encode())
#                 return output


#         # Call `generate_json_ref` for all subfiles in the dataset and
#         # use Dask to write the .json reference files in parallel.
#         tasks = [dask.delayed(generate_json_ref)(subfile, temp_dir, self.fs, so) for subfile in subfiles]
#         dask.compute(tasks)
#         dataset_files = glob.glob(f"{temp_dir}/*.json")


#         # Combine individual reference files into a single dataset reference file. This file will be used to read the
#         # NetCDF datasets (divided into subfiles) from cloud storage.
#         mzz = MultiZarrToZarr(dataset_files, concat_dims=coord_types['concat'], identical_dims=coord_types['identical'])

#         # Write the virtual dataset to local storage
#         os.mkdir(self.benchmark_dir + "/references")
#         with open(f"{self.local_reference}", "wb") as f:
#             f.write(ujson.dumps(mzz.translate()).encode())


#         temp_dir.cleanup() # Remove temporary files
#         print("Virtual dataset of" + file.split('/')[-2] + f'written to {output_filename}')


#     def cloud_upload(self):
#         if not os.path.isfile(self.local_reference):
#             self.generate()

#         self.fs.put_file(self.local_reference, self.remote_reference, **self.storage_options)
#         print(f'Virtual dataset uploaded to {self.remote_reference}')


#     def load(self):
#         if not self.fs.exists(self.remote_reference):
#             self.cloud_upload()

#         uri_prefix = self.base_uri.split(':')[0]
#         tmp_fs = fsspec.filesystem("reference",
#                                     fo=self.remote_reference,
#                                     remote_protocol=uri_prefix,
#                                     **self.storage_options)
#         print(tmp_fs.exits(self.remote_reference))

#         mapper = tmp_fs.get_mapper("")
#         return xr.open_dataset(mapper, engine='zarr', consolidated = False)







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

    def __init__(self, time_desc='runtime_seconds'):
        """This class is used to time a wide range of operations
        in the benchmarking: preprocessing, reading, performing
        mathematical operations, etc. It stores the data 

        Parameters
        ----------
        time_desc : str (default = 'runtime_seconds')
            Describes which operation is being timed. For example,
            a good title for the time it takes for data to be read
            from the cloud might be `time_desc='
        """
        self.time_desc = time_desc
        self.diagnostics = []
        self.tmp_list = []
        

    @contextmanager
    def time(self, array_size=None, use_tmp_list=False, **kwargs):
        """Records execution time for Python commands
        executed under its call

        Parameters
        ----------
        array_size : float (default = None)
            If throughput values are a desired computation, pass the size
            of the Dask array being read from cloud storage
        use_tmp_list : bool (default = False)
            If this bool is set to true, a secondary list
            that can be reset will be used. In the context
            of the benchmarking, this will be used to record
            only the timing values (which will be used to find
            the errors) 
        **kwargs : kwarg
            Pass any number of keyword arguments to be recorded
            alongside the execution time
        """

        tic = time.time()
        yield
        toc = time.time()

        kwargs[self.time_desc] = toc - tic

        if array_size != None:
            kwargs['throughput_MBps'] = array_size / 1e6 / kwargs[self.time_desc]

        if use_tmp_list:
            self.tmp_list.append(kwargs['throughput_MBps'])
        else:
            self.diagnostics.append(kwargs)


    def compute_stats(self, array_size):
        """
        Computes the means and errors of consecutive reads/
        operations with the same number of workers.
        """
        throughputs = np.array(self.tmp_list + [self.diagnostics[-1]['throughput_MBps']])

        mean_throughput = np.mean(throughputs)
        std_dev_thrput = tstd(throughputs)
        self.diagnostics[-1]['throughput_MBps'] = mean_throughput
        self.diagnostics[-1]['throughput_std_dev'] = std_dev_thrput
        self.diagnostics[-1][self.time_desc] = array_size / 1e6 / mean_throughput


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


    def reset_lists(self, reset_tmp=False, reset_diagnostics=False):
        "Resets temporary diag timer list"
        if reset_tmp:
            self.tmp_list = []
        
        if reset_diagnostics:
            self.diagnostics = []







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






class ComputeDetails:
    """
    Provides details about the number
    of threads, cores, and workers being used
    at any point in time

    Attributes
    ----------
    client : Dask client object
        The Dask client that 

    Methods
    -------
    total_nthreads()
        Determines the current amount of threads
        registered with the Dask scheduler
    total_ncores()
        Determines the current cores registered
        with the Dask scheduler
    total_workers()
        Determines the total number of Dask
        workers registered with the scheulder.
        Note that Dask workers are different
        from worker nodes.
        
    """
    def __init__(self, client):
        self.client = client

    def total_nthreads(self):
        return sum([v for v in self.client.nthreads().values()])

    def total_ncores(self):
        return sum([v for v in self.client.ncores().values()])

    def total_workers(self):
        return len(self.client.ncores())