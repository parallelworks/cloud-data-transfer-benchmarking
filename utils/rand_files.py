#####################SETUP#######################
# Imports
import netCDF4 as nc
import numpy as np
import pandas as pd
import math
import sys
import os

# Inputs
csv_switch = 1
netcdf_switch = 0
binary_switch = 0
filesize = 1 # GB
root = 'gs://cloud-data-benchmarks/'

# Define Constants
GB2Byte = 1073741824 # Gigabyte to byte conversion factor

######################FILE GENERATION######################
# Random CSV Generation
if csv_switch:
    np.random.seed(0)
    num_count = filesize*GB2Byte/8 # Divide by 8 bytes/number for number of data points
    df_size = round(math.sqrt(num_count)) # Determine the dimension of a square dataframe
    df = pd.DataFrame(np.random.randn(df_size,df_size)) # Populate a dataframe of dimension
                                                        # `df_size`x`df_size` with random numbers
    df.to_csv(root)