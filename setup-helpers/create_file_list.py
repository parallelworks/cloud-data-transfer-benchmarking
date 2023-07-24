"""Benchmark File List Writer

This script writes a .json file containing paths and filenames
that benchmarking data can be referenced at. These files
will be stored in every cloud object storage location used
for the benchmarking, so the URI of these respective locations
is not used
"""

# Imports
import json
import os

# Define function to get filenames from randomly-generated files and userfiles
def get_filenames(userfiles, randfiles, supported_formats):
    "Get files of a certain format from input list"

    for file_format in supported_formats:
        file_list = []

        for userfile in userfiles:
            # If userfile is one of the supported formats, add it to the list
            if userfile['Format'] == file_format:

                # Separate filename from URI that the user input
                split_path = [ele for ele in userfile['Path'].split('/') if len(ele) != 0]
                filename = split_path[-1]

                # If filename is `*`, change the filename to be `<filename>/*`
                match filename:
                    case '*':
                        filename = f'{split_path[-2]}/{filename}'

                file_list.append(f'cloud-data-transfer-benchmarking/userfiles/{filename}')

        # Since randfiles were created with the correct path, simply check if it was generated
        for randfile in randfiles:
            if randfile['Generate'] and randfile['Format'] == file_format:
                file_list.append(randfile['Path']) 

        yield file_list
    


with open('benchmark_info.json', 'r') as infile:
    user_input = json.loads(infile.read())

userfiles = user_input['USERFILES']
randfiles = user_input['RANDFILES']
supported_formats = ['CSV', 'NetCDF4', 'Binary', 'Parquet', 'Zarr']

file_list = list(get_filenames(userfiles, randfiles, supported_formats))
output_list = dict(zip(supported_formats, file_list))


output_json = json.dumps(output_list)
with open('benchmarks-core/file_list.json', 'w') as outfile:
    outfile.write(output_json)