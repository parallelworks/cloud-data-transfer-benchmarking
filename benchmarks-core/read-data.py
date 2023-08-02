import ujson
import os
import core_helpers as core
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import dask.dataframe as dd
import dask.array as da
import dask
import pandas as pd
import intake_xarray
import xarray as xr
import numpy as np
import time
import fsspec


                    # DEFINE HELPER FUNCTION #
#################################################################
def MainLoop(dask_array, max_workers, diag_kwargs, worker_step=2, tests=5):
    """
    Loop used for cloud storage throughput benchmarking. This function
    is a nested loop that takes a Dask array and scales a cluster to
    test the read throughput of different data formats. By supplying
    `worker_step`, you can also determine the granularity of
    the benchmarking resutls.

    Parameters
    ----------
    dask_array : Dask ndarray
        A chunked Dask array representing lazily loaded data from a cloud
        object store
    max_workers : int
        The maximum amount of Dask workers to scale the cluster up to for read testing
    worker_step : int (default = 2)
        The value by which to change the number of Dask workers.
    test : int (default = 5)
        The amount of reads to perform for each worker count. Note that
        for workers greater than 1, the standard deviation of the throughput,
        as well as the mean execution time and throughput will be computed
        and shown as a single line in the output data.
    """
    for nworkers in np.arange(max_workers, 0, -worker_step):

        array_size = dask_array.nbytes # Get the size of the dask array in bytes
        nworkers = int(nworkers)

        cluster.scale(nworkers)
        time.sleep(10)
        client.wait_for_workers(nworkers)
        print(f'Active Workers: {nworkers}')

        for i in range(tests):
            if i == (tests-1):
                use_tmp_list = False
            else:
                use_tmp_list = True

            with diag_timer.time(array_size=array_size,
                                use_tmp_list=use_tmp_list,
                                nworkers=compute_details.total_workers(),
                                nthreads=compute_details.total_nthreads(),
                                ncores=compute_details.total_ncores(),
                                **diag_kwargs):
                future = da.store(dask_array, null_store, lock=False, compute=False)
                dask.compute(future, retries=5)

        diag_timer.compute_stats()
        diag_timer.reset_lists(reset_tmp=True)
#################################################################



                        # GET INPUTS #
#################################################################
# Set home directory variable
home = os.path.expanduser('~')
benchmark_dir = f'{home}/cloud-data-transfer-benchmarking'
input_dir = f'{benchmark_dir}/inputs'

# Index that indicates which resource to pull cluster options from
resource_index = int(os.environ['resource_index'])

# Open benchmark information file
with open(f'{input_dir}/inputs.json') as infile:
    inputs = ujson.loads(infile.read())

# Open list of files to use in benchmarking
with open(f'{input_dir}/file_list.json', 'r') as infile:
    file_list = ujson.loads(infile.read())

# Populate variables from input file
stores = inputs['STORAGE']
resource = inputs['RESOURCES'][resource_index]
resource_name = resource['Name']
resource_csp = resource['CSP']
dask_options = resource['Dask']
max_nodes = dask_options['MaxNodes']
max_workers = max_nodes
#################################################################


                        # CLUSTER SETUP #
#################################################################
# TODO: Limit more powerful clusters to use the same amount of
# resources as the least-powerful cluster in the benchmarking.
if __name__ == '__main__':
    cores = dask_options['CPUs']
    memory = dask_options['Memory']
    memory = f'{int(round(memory))} GB'
    dask_dir = '/mnt/shared/dask/read-data/dask-worker-logs'

    match dask_options['Scheduler']:
        case 'SLURM':
            cluster = SLURMCluster(account='convert',
                                queue=dask_options['Partition'],
                                job_cpu=cores,
                                cores=cores,
                                memory=memory,
                                processes=1,
                                job_directives_skip=['--mem'],
                                walltime='01:00:00',
                                log_directory=dask_dir
                                    )
    client = Client(cluster)
#################################################################


                        # READ FORMATS #
#################################################################

# Instantiate benchmarking classes
diag_timer = core.DiagnosticTimer(time_desc='read_time')
null_store = core.DevNullStore()
compute_details = core.ComputeDetails(client)


# Begin conversion process
for store in stores:

    # Get information about object store
    base_uri = store['Path']
    csp = store['CSP']
    bucket_type = store['Type']
    storage_options = store['Credentials']
    
    # Change path of credentials files to match location on cluster
    if bucket_type == "Private" and csp == "GCP":
        storage_options['token'] = benchmark_dir + '/storage-keys/' + storage_options['token'].split('/')[-1]



    # Get storage options to pass into conversion functions as a kwarg. Also
    # set the filesystem used in the current bucket
    fs = fsspec.filesystem(base_uri.split(':')[0], **storage_options)

    # Print message saying that conversion is beginning
    print(f'Reading files in \"{base_uri}\" with \"{resource_name}\"...')



            # READ CSV #
    ##############################
    for file in file_list['CSV']:
        # First, check for userfiles and set the correct path
        # for the current storage location
        filename = core.check_for_userpath(file, base_uri)
        dataset_name = core.get_dataset_name(filename)


        if filename[-1] == '*':
            csv_format = 'CSV_subfiles'
        else:
            csv_format = 'CSV'



        # Stage dataframe for read operation by loading as a Dask array
        df = dd.read_csv(f'{base_uri}/{filename}', assume_missing=True, header=None, storage_options=storage_options)
        print('Computing array column lengths...')
        dask_array = df.to_dask_array(lengths=True)
        print('Done')
        chunksize = np.prod(dask_array.chunksize) * dask_array.dtype.itemsize



        # Convert the CSV file to parquet and time the execution
        #print(f'Converting {dataset_name} to Parquet...')
        diag_kwargs = dict(resource=resource_name,
                           resource_csp=resource_csp,
                           bucket=base_uri,
                           bucket_csp=csp,
                           fileFormat=csv_format,
                           original_dataset_name=dataset_name,
                           nbytes=dask_array.nbytes,
                           chunksize=chunksize)
        print(f'Reading {filename}...')
        MainLoop(dask_array, max_workers, diag_kwargs)




            # READ PARQUET #
    ##############################
    for file in file_list['Parquet']:


        filename = core.check_for_userpath(file, base_uri)
        dataset_name = core.get_dataset_name(filename)



        df = dd.read_parquet(f'{base_uri}/{filename}', storage_options=storage_options)



        print('Computing array column lengths...')
        dask_array = df.to_dask_array(lengths=True)
        print('Done.')



        chunksize = np.prod(dask_array.chunksize) * dask_array.dtype.itemsize
        diag_kwargs = dict(resource=resource_name,
                    resource_csp=resource_csp,
                    bucket=base_uri,
                    bucket_csp=csp,
                    fileFormat='Parquet',
                    original_dataset_name=dataset_name,
                    nbytes=dask_array.nbytes,
                    chunksize=chunksize)
        print(f'Reading {filename}...')
        MainLoop(dask_array, max_workers, diag_kwargs)


            # READ NETCDF4 #
    #################################
    for file in file_list['NetCDF4']:


        # First, check for userfiles and set the correct path
        # for the current storage location
        filename = core.check_for_userpath(file, base_uri)


        # Get the dataset name and set the upload path
        dataset_name = core.get_dataset_name(filename)



        # If a globstring is specified, the files must be combined by a custom function
        if filename[-1] == '*':
            ds = core.combine_nc_subfiles(base_uri, filename, storage_options, fs)
            netcdf_format = 'NetCDF4_subfiles'
        # Otherwise, we can load the entire NetCDF file from cloud storage in a single command
        else:
            ds = intake_xarray.netcdf.NetCDFSource(f'{base_uri}/{filename}', storage_options=storage_options).to_dask()
            netcdf_format = 'NetCDF4'

        

        data_vars = [v for v in ds.data_vars]
        for data_var in data_vars:

            # If dataset is a single file, change dask chunks to match internal
            # chunks. In both sc
            if filename[-1] != '*':
                coords = ds[data_var].dims
                chunks = ds[data_var].encoding['chunksizes']
                dask_array = ds[data_var].chunk(chunks=dict(zip(coords, chunks))).data
            else:
                dask_array = ds[data_var].data

            chunksize = np.prod(dask_array.chunksize) * dask_array.dtype.itemsize

            diag_kwargs = dict(resource=resource_name,
                            resource_csp=resource_csp,
                            bucket=base_uri,
                            bucket_csp=csp,
                            fileFormat=netcdf_format,
                            original_dataset_name=f'{dataset_name}-{data_var}',
                            nbytes=dask_array.nbytes,
                            chunksize=chunksize)
            print(f'Reading the variable {data_var} from {filename}...')
            MainLoop(dask_array, max_workers, diag_kwargs)

            # READ ZARR #
    ##############################
    for file in file_list['Zarr']:


        filename = core.check_for_userpath(file, base_uri)
        dataset_name = core.get_dataset_name(filename)


        ds = xr.open_zarr(f'{base_uri}/{filename}', consolidated=True, storage_options=storage_options)


        data_vars = [v for v in ds.data_vars]
        for data_var in data_vars:

            dask_array = ds[data_var].data
            chunksize = np.prod(dask_array.chunksize) * dask_array.dtype.itemsize

            diag_kwargs = dict(resource=resource_name,
                            resource_csp=resource_csp,
                            bucket=base_uri,
                            bucket_csp=csp,
                            fileFormat='Zarr',
                            original_dataset_name=f'{dataset_name}-{data_var}',
                            nbytes=dask_array.nbytes,
                            chunksize=chunksize)
            print(f'Reading the variable {data_var} from {filename}...')
            MainLoop(dask_array, max_workers, diag_kwargs)
#################################################################

# Close Dask client and shut down worker nodes
print('Shutting down worker nodes...')
cluster.scale(0)
client.close()
print('Workers shut down. (this may take a while to register in the platform UI)')


# Load dataframe and save it to a CSV file
df = diag_timer.dataframe()
results_path = f'{benchmark_dir}/outputs/results_tmp.csv'
if resource_index == 0:
    df.to_csv(results_path, index=False)
else:
    df.to_csv(results_path, header=None, index=False)