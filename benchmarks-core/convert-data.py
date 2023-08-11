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
import ujson
import os
import sys
import core_helpers as core
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import intake_xarray
import xarray as xr
import fsspec
import copy
import preprocessing_helpers as preproc


                        # GET INPUTS #
#################################################################
# Set home directory variable
home = os.path.expanduser('~')
benchmark_dir = f'{home}/cloud-data-transfer-benchmarking'
input_dir = f'{benchmark_dir}/inputs'


# Index that indicates which resource to pull cluster options from
resource_index = int(os.environ['resource_index'])


# Open benchmark information file
with open(f'{input_dir}/inputs.json', 'r') as infile:
    inputs = ujson.loads(infile.read())


# Populate variables from input file
stores = inputs['STORAGE']
file_list = inputs['FILELIST']
convert_options = inputs['CONVERTOPTS']
resource = inputs['RESOURCES'][resource_index]

# Resource-specific information
resource_name = resource['Name']
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
    dask_dir = '/mnt/shared/dask/convert-data/dask-worker-logs'

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

# Instantiate helper classes
diag_timer = core.DiagnosticTimer(time_desc='conversion_time') # Instantiate diagnostic timer

# Set path that cloud native files will be written to and update file list flag 
cloud_native_path = 'cloud-data-transfer-benchmarking/cloudnativefiles/' # Path to write data to
list_cloud_native_path = copy.copy(cloud_native_path)
update_file_list = True # flag to update file list with newly written files


# If files have already been written to the selected cloud storage
# locations, put new writes in a different directory to be deleted
if resource_index > 0:
    cloud_native_path= 'cloud-data-transfer-benchmarking/tmp/'




# Scale cluster up to maximum
cluster.scale(max_nodes)
print('Waiting for worker nodes to start up...')
client.wait_for_workers(max_nodes)
print('Workers active.\n\n')





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

    print(f'Converting files in \"{base_uri}\" with \"{resource_name}\"...\n')




    # Convert files to Parquet format
    for file in file_list['CSV']:

        # First, check for userfiles and set the correct path
        # for the current storage location
        filename = core.check_for_userpath(file, base_uri)
        

        # Set the name function for the parquet subfiles and
        # get the dataset name from the filepath.
        name_function = lambda x: f'part{x}.parquet'
        dataset_name = core.get_dataset_name(filename)



        for option_set in convert_options:
            if dataset_name in option_set['Datasets']:

                # Load CSV dataset
                df = dd.read_csv(f'{base_uri}/{filename}', assume_missing=True, header=None, storage_options=storage_options)
                df = df.rename(columns=str)

                # Rechunk
                chunksize=option_set['Chunksize']
                df = df.repartition(partition_size=f'{chunksize}MB')

                # Make copy of storage options and set compression level to pass when conversion is executed
                lvl = option_set['Level']
                so = copy.copy(storage_options)
                so['compression_level'] = lvl

                # Loop through algorithms
                for algorithm in option_set['Algorithms']:
                    # Set paths for upload and recording into the file list
                    upload_path = cloud_native_path + f'{dataset_name}_{float(chunksize)}MB_{algorithm}_{lvl}.parquet'
                    list_upload_path = list_cloud_native_path + f'{dataset_name}_{float(chunksize)}MB_{algorithm}_{lvl}.parquet' # Path to update file list

                    # Convert the CSV file to parquet and time the execution
                    print(f'Converting \"{dataset_name}\" with {chunksize}MB chunks & level {lvl} {algorithm} compression to Parquet...')
                    diag_kwargs = dict(resource=resource_name,
                                    resource_csp=resource_csp,
                                    bucket=base_uri,
                                    bucket_csp=csp,
                                    conversionType='CSV-to-Parquet',
                                    orig_dataset_name=dataset_name,
                                    data_vars='N/A',
                                    compr_alg=algorithm,
                                    compr_lvl=lvl,
                                    chunksize_MB=chunksize)

                    with diag_timer.time(**diag_kwargs):
                        df.to_parquet(f'{base_uri}/{upload_path}', name_function=name_function, compression=algorithm, storage_options=so)

                    print(f'Written to \"{base_uri}/{upload_path}\"')


                    # Update file list
                    if update_file_list:
                        file_list['Parquet'].append(f'{list_upload_path}/*')





    # Convert  files to Zarr format
    for file in file_list['NetCDF4']:
        filenames = file['Path']
        data_vars = file['DataVars']

        # First, check for userfiles and set the correct path
        # for the current storage location
        filename = core.check_for_userpath(filenames, base_uri)

        # Get the dataset name
        dataset_name = core.get_dataset_name(filename)

        # If a globstring is specified, the files must be combined by a custom function
        if filename[-1] == '*':
            # virtual_dataset = core.virtual_dataset(base_uri, filename, storage_options, fs)
            # ds = virtual_dataset.load()
            pass

        # Otherwise, we can load the entire NetCDF file from cloud storage in a single command
        else:
            ds = intake_xarray.netcdf.NetCDFSource(f'{base_uri}/{filename}', storage_options=storage_options).to_dask()

            # The chunksizes of the resulting Dask array must match the internal chunks of the NetCDF file for 
            # an efficient conversion. This auomatically sets the chunking scheme based on that of the first data
            # variable in the NetCDF file.


        # Apply chunksize and compression schemes
        for option_set in convert_options:
            if dataset_name in option_set['Datasets']:

                # Assume data variable input is '*'
                dvars = [v for v in ds.data_vars]
                file_list_dvars = ['*']

                # If it is not '*', change the data var list
                if data_vars[0] != '*':
                    dvars = data_vars
                    file_list_dvars = copy.copy(dvars)
                
                chunksize = option_set['Chunksize']
                ds = preproc.dataset_rechunk(ds, dvars, chunksize) # Rechunk data

                # Loop through compression algorithms in current option set and write
                lvl = option_set['Level'] # Compression level for all algorithms in option set
                for alg in option_set['Algorithms']:


                    so = copy.copy(storage_options) # Copy storage options so we don't change the original dictionary
                    so['compressor'] = preproc.zarr_compression(algorithm=alg, level=lvl) # Get compressor

                    # Set upload paths with compressor and chunksize information built into the name
                    upload_path = cloud_native_path + f'{dataset_name}_{float(chunksize)}MB_{alg}_{lvl}.zarr'
                    list_upload_path = list_cloud_native_path + f'{dataset_name}_{float(chunksize)}MB_{alg}_{lvl}.zarr'

                    # Convert the NetCDF4 file to Zarr and record the results
                    data_var_string = ', '.join(dvars)
                    print(f'Converting data variables \"{data_var_string}\" from \"{dataset_name}\" with {chunksize}MB chunks & level {lvl} {alg} compression to Zarr...')
                    diag_kwargs = dict(resource=resource_name,
                                    resource_csp=resource_csp,
                                    bucket=base_uri,
                                    bucket_csp=csp,
                                    conversionType='NetCDF-to-Zarr',
                                    orig_dataset_name=dataset_name,
                                    data_vars=', '.join(file_list_dvars),
                                    compr_alg=alg,
                                    compr_lvl=lvl,
                                    chunksize_MB=chunksize)

                    with diag_timer.time(**diag_kwargs):
                        ds.to_zarr(store=f'{base_uri}/{upload_path}', storage_options=so, consolidated=True)

                    print(f'Written to \"{base_uri}/{upload_path}\"')

                    # Update file list
                    if update_file_list:
                        file_list['Zarr'].append({'DataVars': file_list_dvars, 'Path': list_upload_path})


    print(f'Done converting files in \"{base_uri}\".')
    update_file_list = False # Stop updating file list with repeated uploads

    # Since we are testing conversions with all resources, files will
    # be written to all cloud storage locations more than once. To
    # prevent ballooning of the size of these stores, we remove any
    # redudant files after they were written the first time.
    if resource_index > 0:
        fs.rm(f'{base_uri}/{cloud_native_path}', recursive=True)
#################################################################

# Close Dask client and shut down worker nodes
print('Shutting down worker nodes...')
cluster.scale(0)
client.close()
print('Workers shut down. (this may take a while to register in the platform UI)')

# # Load dataframe and save it to a CSV file
df = diag_timer.dataframe()
results_path = f'{benchmark_dir}/outputs/results-convert-data.csv'
if resource_index == 0:
    df.to_csv(results_path, index=False)
else:
    df.to_csv(results_path, header=None, index=False)


# Write updated file list back to `file_list.json`
updated_json = ujson.dumps(inputs)
with open(f'{input_dir}/inputs.json', 'w') as outfile:
    outfile.write(updated_json)