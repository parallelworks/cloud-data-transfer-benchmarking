#####################SETUP#######################
# Imports
import gcsfs
import s3fs
import xarray as xr
import numpy as np
import pandas as pd
import math
import sys
import os
import json

# Shell Inputs
## Desired file types to create. Input will be passed in the form shown below
## RANDGEN_FILES="<csv_int> <netcdf_int> <binary_int>"
file_types = tuple(json.loads(os.environ['RANDGEN_FILES']))

## Desired file sizes to create. Order matches the respective file type input.
## File sizes represented in GB
file_sizes = tuple(json.loads(os.environ['RANDGEN_SIZES']))

## Desired cloud storage location(s) to write files to
locations = tuple(json.loads(os.environ['RANDGEN_STORES']))

## Define Functions
class fileGen:
    def __init__(self):
        np.random.seed(0)
        self.filename = 'randfile'
        self.GB2Byte = 1073741824 # Gigabyte to byte conversion factor

    def initGCSFS(self):



    def initS3FS(self):
        pass

    def csvGen(self, filesize, location, token):
        print('Generating CSV...')
        n_b = 19 # average byte size of expected randomly generated numbers
        c_b = 1 # byte size of each comma delimited
        df_size = round((c_b+math.sqrt(c_b**2 + 4*(c_b+n_b)*(self.GB2Byte*filesize)))/(2*(n_b+c_b)))
        df = pd.DataFrame(np.random.randn(df_size,df_size)) # Populate a dataframe of dimension
                                                            # `df_size`x`df_size` with random numbers
        upload_path = location + self.filename + '_' + str(filesize) + 'GB.csv'
        if token == 'None':
            df.to_csv(upload_path, header=None, index=False)
        else:
            df.to_csv(upload_path, header=None, index=False, storage_options={'token':token})
        print('CSV of size', filesize, 'written to', location)

    def netcdfGen(self, filesize, location, token):
            pass

    def binaryGen(self, filesize, location, token):
            pass

    def edit_loc_strings(self, loc_strings):
        for i in range(len(loc_strings)):
            current = loc_strings[i]
            if current[-1] != '/':
                loc_strings[i] = current + '/'

        return tuple(loc_strings)

    def write_files(self, filetypes, filesize, locations, tokens):
        print('Editing locations')
        locations = self.edit_loc_strings(list(locations))
        print('Locations edited')

        for i in range(len(filetypes)):
            current_file = filetypes[i]
            current_size = filesize[i]
            for n in range(len(locations)):
                match current_file:
                    case "CSV":
                        print('Calling CSV')
                        self.csvGen(current_size, locations[n], tokens[n])
                    case "NetCDF4":
                        self.netcdfGen(current_size, locations[n], tokens[n])
                    case "Binary":
                        self.binaryGen(current_size, locations[n], tokens[n])

fileGen = fileGen()

######################FILE GENERATION######################
# For now, tokens are generated as empty until an option to add
# cloud storage token location is introduced to the workflow
tokens = []
for i in range(len(locations)):
    tokens.append('None')
tokens = tuple(tokens)

#fileGen.write_files(file_types, file_sizes, locations, tokens)