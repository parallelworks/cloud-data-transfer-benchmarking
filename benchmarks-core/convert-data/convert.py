"""Convert Legacy Formats to Cloud-Native

This script carries out the process of converting applicable
user input files and randomly generated files to cloud-native
formats.

 CSV and NetCDF4 data will be converted to Parquet and
Zarr, respectively. The time it takes for this conversion process
to happen will be recored and output to a CSV file that will be
used for plotting later in the benchmark.

This script will be run on each cluster in the benchmarking
(in serial).
"""

# Imports
import json
import os
import sys
home = os.path.expanduser('~')
sys.path.insert(0, f'{home}/benchmarks-core')
import core_helpers as core
from dask_jobqueue import SLURMCluster
import dask
from dask.distributed import Client
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import intake_xarray
import xarray as xr
import zarr


                    # DEFINE HELPER FUNCTIONS #
#################################################################
def get_dataset_name(file):
    "Function that gets a dataset name from the filepath"
    split_path = [ele for ele in file.split('/') if len(ele)!=0]
    filename = split_path[-1]
    match filename:
        case '*':
            return split_path[-2]
        case other:
            return filename
#################################################################



                        # GET INPUTS #
#################################################################
# Index that indicates which resource to pull cluster options from
resource_index = int(os.environ['resource_index'])

# Open benchmark information file
with open(f'{home}/benchmarks-core/benchmark_info.json') as infile:
    inputs = json.loads(infile.read())

# Open list of files to use in benchmarking
with open('file_list.json', 'r') as infile:
    file_list = json.loads(infile.read())

# Populate variables from input file
stores = inputs['STORAGE']
resource = inputs['RESOURCES'][resource_index]
resource_csp = resource['CSP']
dask_options = resource['Dask']
max_nodes = dask_options['MaxNodes']
#################################################################


                        # CLUSTER SETUP #
#################################################################
# TODO: Limit more powerful clusters to use the same amount of
# resources as the least-powerful cluster in the benchmarking.
if __name__ == '__main__':
    cores = dask_options['CPUs']
    memory = dask_options['Memory']
    memory = f'{int(round(memory))} GB'
    dask_dir = '/mnt/shared/dask-worker-logs'

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


                        # CONVERT FORMATS #
#################################################################
# TODO: Add different compression engine and chunksize support

# Instantiate diagnostic timer class
diag_timer = core.DiagnosticTimer(time_desc='Conversion Time')

# Common path that all cloud-native formats will be stored in.
# This will exist in all benchmarking buckets
cloud_native_path = 'cloud-data-transfer-benchmarking/cloudnativefiles/'

# Flag to update file list with newly written file
update_file_list = True

# Scale cluster up to maximum
cluster.scale(max_nodes)
client.wait_for_workers(max_nodes)

# Begin conversion process
for store in stores:
    # If files have already been written to the selected cloud storage
    # locations, put new writes in a different directory to be deleted
    if resource_index > 0:
        cloud_native_path= 'cloud-data-transfer-benchmarking/tmp/'


    # Get information about object store
    base_uri = store['Path']
    csp = store['CSP']
    bucket_type = store['Type']
    credentials = store['Credentials']
    
    # Change path of credentials files to match location on cluster
    if bucket_type == "Private" and csp == "GCP":
        credentials = home + '/benchmarks-core/' + credentials.split('/')[-1]

    # Get storage options to pass into conversion functions as a kwarg. Also
    # set the filesystem used in the current bucket
    storage_options = core.get_storage_options(csp, bucket_type, credentials)
    fs = core.SetFileSystem(store, csp, bucket_type, storage_options)


    # Convert files to Parquet format
    for file in file_list['CSV']:
        
        name_function = lambda x: f'part{x}.parquet'
        dataset_name = get_dataset_name(file)
        upload_path = cloud_native_path + f'{dataset_name}_parquet'
        df = dd.read_csv(f'{store}/{file}', assume_missing=True, header=None)

        with diag_timer.time(conversion_type = 'csv2parquet', dataset_name=dataset_name):
            dd.to_parquet(df, f'{base_uri}/{upload_path}', name_function=name_function, storage_options=storage_options)
        del df

        # Update file list
        if update_file_list:
            file_list['Parquet'].append(upload_path)



    # Convert  files to Zarr format
    for file in file_list['NetCDF4']:
        dataset_name = get_dataset_name(file)
        upload_path = cloud_native_path + f'{dataset_name}_zarr'

        if file[-1] == '*':
            ds = core.combine_nc_subfiles(base_uri, file, storage_options, fs)
        else:
            ds = intake_xarray.netcdf.NetCDFSource(f'{store}/{file}', storage_options=storage_options).to_dask()
            data_vars = [v for v in ds.data_vars]
            dims = ds[data_vars[0]].dims
            chunks = ds[data_vars[0]].encoding['chunksizes']
            ds = ds.chunk(chunks=dict(zip(dims, chunks)))
        
        with diag_timer.time(conversionType='netcdf2zarr', dataset_name=dataset_name):
            ds.to_zarr(store=f'{base_uri}/{upload_path}', storage_options=storage_options, 
                    consolidated=True)
        del ds

        if update_file_list:
            file_list['Zarr'].append(upload_path)

    update_file_list = False
    if resource_index > 0:
        fs.rm(f'{base_uri}/{cloud_native_path}', recursive=True)
#################################################################

# Close Dask client and shut down worker nodes
cluster.scale(0)
client.close()

# Write updated file list back to `file_list.json`
updated_json = json.dumps(file_list)
with open(f'{home}/benchmarks-core/file_list.json', 'w') as outfile:
    outfile.write(updated_json)