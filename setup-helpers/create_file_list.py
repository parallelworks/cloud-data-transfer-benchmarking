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

        # USER-DEFINED FILES SECTION
        # Get the base dataset names of all Source Paths provided in the userfiles section of the benchmark info
        current_userfiles = [file['SourcePath'] for file in userfiles if file['Format'] == file_format] # Files that match the current format
        
        if file_format == 'NetCDF4' or file_format == 'Zarr':
            current_datavars = [file['DataVars'] for file in userfiles if file['Format'] == file_format]

        split_paths = [path.split('/') for path in current_userfiles] # Split the URI path of each userfile
        tmp_names = [name[-1] for name in split_paths] # Store temporary names of the datasets. Could be a filename or "*""

        # Check for `*`. If so, take the second-to-last element of the split path as the dataset identifier
        for i, tmp_name in enumerate(tmp_names):
            if tmp_name == '*':
                tmp_name = f'{split_paths[i][-2]}/{tmp_name}'
        
        # Populate a dictionary containing unique dataset names and the indices at which they occur in `tmp_names`
        user_datasets = {}
        for i, name in enumerate(tmp_names):
            if name not in user_datasets:
                user_datasets[name] = [i]
            elif name in user_datasets:
                user_datasets[name].append(i)

        # Iterate through the dataset dictionary and store all the original URIs of a unique dataset.
        # After this, append the list of all possile storage locations of that dataset to a list of
        # the current file format
        for key, value in user_datasets.items():
            if file_format == 'NetCDF4' or file_format == 'Zarr':
                tmp_list = {'DataVars' : current_datavars[value[0]], 'Path' : [current_userfiles[index] for index in value]}
                tmp_list['Path'].append(f'cloud-data-transfer-benchmarking/userfiles/{key}')
            else:
                tmp_list = [current_userfiles[index] for index in value]
                tmp_list.append(f'cloud-data-transfer-benchmarking/userfiles/{key}')

            file_list.append(tmp_list)



        # RANDOMLY-GENRATED FILES SECTION
        # Since randfiles were created with the correct path, simply check if it was generated
        for randfile in randfiles:
            if randfile['Generate'] == True and randfile['Format'] == file_format:
                size = randfile['SizeGB']
                filename = f'random_{float(size)}GB_{file_format}'

                if file_format == 'NetCDF4':
                    append_term = {'DataVars' : ['*'], 'Path' : f'cloud-data-transfer-benchmarking/randfiles/{filename}.nc'}
                else:
                    append_term = f'cloud-data-transfer-benchmarking/randfiles/{filename}/*'

                file_list.append(append_term)

        yield file_list
    

# Load user input .json file
with open('inputs.json', 'r') as infile:
    user_input = json.loads(infile.read())

# Populate variabels with information about user-defined datasets
# and randomly-genrated data
userfiles = user_input['USERFILES']
randfiles = user_input['RANDFILES'][:-1]

# Set list of supported formats. If more formats are added to the
# benchmarking, the new formats will need to be added to this list
supported_formats = ['CSV', 'NetCDF4', 'Parquet', 'Zarr']

# Call generator function to get the overall file list. Form this
# into a dictionary that contains info about all files to be used
# in the benchmarking and their respective format
file_list = list(get_filenames(userfiles, randfiles, supported_formats))
user_input['FILELIST'] = dict(zip(supported_formats, file_list))

# Write file list to `benchmarks-core` to be used in read/write
# operations in the benchmarking
output_json = json.dumps(user_input)
with open('inputs.json', 'w') as outfile:
    outfile.write(output_json)