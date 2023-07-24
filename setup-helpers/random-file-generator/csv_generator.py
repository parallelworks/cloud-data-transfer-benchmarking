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


def get_storage_options(csp, bucket_type, credentials):
    """Depending on the cloud service provider and
    bucket type, determines what the value of the
    `storage_options` keyword argument should be
    """
    if bucket_type == 'Public':
        storage_options = {'anon': True}
    elif csp == 'GCP' and bucket_type == 'Private':
        storage_options = {'token': credentials}
    elif csp == 'AWS' and bucket_type == 'Private':
        storage_options = {'anon': False, 'profile': credentials}
    else:
        storage_options = None

    return storage_options


def write(filesize : float, storage_info : dict) -> str:
    """Write CSV file to cloud storage

    Parameters
    ----------
    filesize : float
        Desired size of the randomly generated CSV file (in GB)
    storage_info : dict
        A dictionary containing information about all storage locations
        that randomly-generated files are to be written to.

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


    for n in range(len(storage_info)):

        # Grab info about current cloud storage location
        current_uri = storage_info[n]['Path']
        location = current_uri + '/cloud-data-transfer-benchmarking/randfiles'
        csp = storage_info[n]['CSP']
        credentials = storage_info[n]['Credentials'].split('/')[-1]
        bucket_type = storage_info[n]['Type']

        full_path = f'{location}/{filename}'

        # Get storage options
        storage_options = get_storage_options(csp, bucket_type, credentials)

        # Write CSV file to storage based on bucket type
        if bucket_type == 'PW Mounted':
            os.system(f'mkdir -p {full_path}')
            df.to_csv(f'file:/{full_path}', header=None, index=False)
        else:
            df.to_csv(full_path,
                    header=None,
                    index=False,
                    storage_options=storage_options)


        # Print confirmation message and return path
        print(f'Files written to \"{full_path}\"')

    return f'{filename}*'