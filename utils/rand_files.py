#####################SETUP#######################
# Imports
import netCDF4 as nc
import numpy as np
import pandas as pd
import math
import sys
import os
import json

# Shell Inputs
## Desired file types to create. Input will be passed in the form shown below
## RANDGEN_FILES="<csv_int> <netcdf_int> <binary_int>"
files_types = os.environ['RANDGEN_FILES'].split(' ')

## Desired file sizes to create. Order matches the respective file type input.
## File sizes represented in GB
file_sizes = os.environ['RANDGEN_SIZES'].split(' ')

## Desired cloud storage location(s) to write files to
location = os.environ['CLOUD_STORAGE'].split(' ')

# Define Constants
GB2Byte = 1073741824 # Gigabyte to byte conversion factor

# Define Functions
class fileGen:
    def __init__(self)
        np.random.seed(0)
    def csvGen(self, filesize, switch, location)
        num_count = filesize*GB2Byte/8 # Divide by 8 bytes/number for number of data points
        df_size = round(math.sqrt(num_count)) # Determine the dimension of a square dataframe
        df = pd.DataFrame(np.random.randn(df_size,df_size)) # Populate a dataframe of dimension
                                                            # `df_size`x`df_size` with random numbers
        df.to_csv(root)
    def netcdfGen(self, filesize, switch, location)
        pass
    def binaryGen(self, filesize, switch, location)
        pass

    file_gen = fileGen()

######################FILE GENERATION######################
for n in location:
    fileGen.csvGen(file_sizes[i], files_types[i], n)
    fileGen.netcdfGen(file_sizes[i], files_types[i], n))
