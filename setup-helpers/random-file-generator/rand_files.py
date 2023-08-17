"""This script orchestrates random file generation using helper
function contained within the files:

    * csv_generator.py
    * netcdf_generator.py
    * binary_generator.py

Most importantly, these file writes occur in parallel using Dask,
allowing the user to perform out-of-memory file writes to use in
benchmark testing. Major consequences of this method are seen in
the files themselves, as CSV and NetCDF files will be split into
many subfiles for Dask to load in. This will most definitely
speed up their parallel read time compared to files that are stored
in one body, but is a compromise that must be made to ensure that
the parallel upload continues smoothly.
"""


# Imports
import os
import ujson
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import csv_generator as csv
import netcdf_generator as netcdf
import binary_generator as binary


# Open user input file
home = os.path.expanduser('~')
input_file = os.environ['input_file']
input_path = f'{home}/cloud-data-transfer-benchmarking/inputs/{input_file}'
with open(input_path, 'r') as infile:
    user_input = ujson.loads(infile.read())

# Pull information about randomly generated files
# and desired resource name
file_info = user_input['RANDFILES'][:-1]
storage_info = user_input['STORAGE']


# Use resource name to grab Dask options for that
# resource. Will be used to setup the SLURM cluster 
rand_resource_name = user_input['RANDFILES'][-1]
for i in user_input['RESOURCES']:
    match i['Name']:
        case rand_resource_name:
            dask_options = i['Dask']
            break

cores = dask_options['CPUs']
memory = dask_options['Memory']
memory = f'{int(memory)} GB'

# Define Dask Resource Manager and initialize scheduler
if __name__ == '__main__':
    dask_dir = '/mnt/shared/dask/randfiles/dask-worker-logs'
    match dask_options['Scheduler']:
        case 'SLURM':
            cluster = SLURMCluster(account='randgen',
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
    cluster.adapt(minimum=0, maximum=dask_options['MaxNodes'])
    



# Main file generation loop. Writes requested
# file formats and sizes to all resources used 
# for benchmarking workflow.
for i in range(len(file_info)):
    # Set file format and size for
    # current loop index
    current_file = file_info[i]['Format']
    current_size = file_info[i]['SizeGB']
    current_generate = file_info[i]['Generate']

    # Will only generate the given file if generation bool is set to `True`
    if current_generate:
        # Pass current variables to the file generator and generate the file based on
        # current file format defined by the main loop
        match current_file:
            case "CSV":
                print('Generating CSV...')
                csv.write(current_size, storage_info)
                print('Done.')

            case "NetCDF4":
                print('Generating NetCDF4...')
                netcdf.write(current_size, storage_info, file_info[i])
                print('Done.')

            case "Binary":
                print('Generating binary file...')
                binary.write(current_size, storage_info)
                print('Done.')
    # End of `generate` conditional
# End of main loop


client.close() # Close SLURM cluster
# END OF SCRIPT