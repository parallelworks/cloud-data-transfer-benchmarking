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
import json
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import csv_generator as csv
import netcdf_generator as netcdf
import binary_generator as binary


# Open user input file
with open('benchmark_info.json', 'r') as infile:
    user_input = json.loads(infile.read())

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
    dask_dir = '/mnt/shared/dask-worker-logs'
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
    



# Preprocessing step to ensure that URI(s) given by user
# have a uniform format
for i in storage_info:
    # Create folder within bucket to hold randomly-generated files
    i['Path'] = i['Path'] + 'cloud-data-transfer-benchmarking/randfiles/'



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
        # Loop through all cloud storage locations
        for n in range(len(storage_info)):

            # Set cloud storage location, credential token (can be 'None'
            # if the location doesn't require a token), and cloud service provider
            # of the object store for the current nested loop index
            current_uri = storage_info[n]['Path']
            current_csp = storage_info[n]['CSP']
            current_token = storage_info[n]['Credentials'].split('/')[-1]
            current_bucket_type = storage_info[n]['Type']

            # Pass current variables to the file generator and generate the file based on
            # current file format defined by the main loop
            match current_file:
                case "CSV":
                    print('Generating CSV...')
                    file_info[0]['Filename'] = csv.write(current_size,
                                                        current_uri,
                                                        current_bucket_type,
                                                        current_csp,
                                                        current_token)
                    print('Done.')
                case "NetCDF4":
                    print('Generating NetCDF4...')
                    file_info[1]['Filename'] = netcdf.write(current_size,
                                                            current_uri,
                                                            current_bucket_type,
                                                            current_csp,
                                                            current_token,
                                                            file_info[i])
                    print('Done.')
                case "Binary":
                    print('Generating binary file...')
                    file_info[2]['Filename'] = binary.write(current_size,
                                                            current_uri,
                                                            current_bucket_type,
                                                            current_csp,
                                                            current_token)
                    print('Done.')
        # End of nested loop
    # End of `generate` conditional
# End of main loop


client.close() # Close SLURM cluster


# Update `benchmark_info.json`
user_input_update = json.dumps(user_input)
with open('benchmark_info.json', 'w') as outfile:
    outfile.write(user_input_update)
# END OF SCRIPT