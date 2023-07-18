import math
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import os
    
def setup(filesize : float):
    """Setup the dataframe that will be used to generate the csv file

        Parameters
        ----------
        filesize : float
            Desired size of the CSV file (in GB)
        location : str
            String of the cloud storage location. Currently, only URIs
            are supported. In the future, mounted cloud object store
            files will be fully supported.

        Returns
        -------
        df : dask dataframe
            A dataframe that the CSV file will be written from
    """

    # Define size constants
    GB2Byte = 1000000000 # Conversion factor from GB to B
    n_b = 19 # Approximate bytes a single value takes up
    c_b = 1 # Bytes a single comma takes up

    # Find dataframe size and generate dataframe
    df_size = round((c_b+math.sqrt(c_b**2 + 4*(c_b+n_b)*(GB2Byte*filesize)))/(2*(n_b+c_b)))
    array = da.random.random((df_size,df_size))
    df = dd.from_array(array)

    return df


def write(filesize : float, location : str, bucket_type : str,  csp : str, token : str) -> str:
    """Write CSV file to cloud storage

    Parameters
    ----------
    filesize : float
        Desired size of the randomly generated CSV file (in GB)
    location : str
        Cloud object store URI or local path to mounted filesystem
    bucket_type : str
        One of three options given from user input in `main.ipynb`:
        Public, Private, or PW Mounted
    csp : str
        Cloud service provider of the bucket
    token : str
        Local path to the cloud storage access token. This will be located within
        the randomly-generated file options folder of the cluster's head node

    Returns
    -------
    full_path : str
        Full path of the randomly generated file written into a provided cloud
        storage location. This information is passed back to the main workflow
        for later use, and will represent either a full URI of the object location
        or POSIX filesystem path corresponding to the location of the object if the
        storage is mounted.
    
    """

    # Generate dataframe
    df = setup(filesize)

    # Set filename
    filename = 'random_' + str(filesize) + 'GB_CSV/'
    full_path = location + filename


    # Write CSV file to storage based on bucket type
    match bucket_type:
        case 'Private':
            # If private bucket, determine which type of credentials to use
            match csp:
                case 'GCP':
                    df.to_csv(full_path,
                            header=None,
                            index=False,
                            storage_options={'token':token})

        case 'PW Mounted':
            os.system(f'mkdir -p {full_path}')
            df.to_csv(f'file://{full_path}', header=None, index=False)

        case other:
            df.to_csv(full_path, header=None, index=False)


    # Print confirmation message and return path
    print(f'Files written to \"{full_path}\"')
    return filename