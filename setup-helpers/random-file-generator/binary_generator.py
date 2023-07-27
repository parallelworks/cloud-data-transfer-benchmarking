import os
import gcsfs
import s3fs
import dask.array as da




def setup(filesize : float) -> int:
    """Find the desired filesize in bytes
    
    Parameters
    ----------
    filesize : float
        The desired size of the randomly generated binary file (in GB)

    Returns
    -------
    round(GB2Byte*filesize) : int
        The desired filesize in bytes
    """

    GB2Byte = 1000000000 # Conversion factor from GB to B
    return round(GB2Byte*filesize)




def SetFileSystem(bucket_type : str, csp : str, credentials : str):
    """Opens a cloud storage filesystem to write to nonmounted locations

    Parameters
    ----------
    bucket_type : str
        One of two options: Private or Public
    csp : str
        Cloud service provider of the bucket. Tells the function which filesystem
        to initialize
    token : str
        Local path to the cloud storage access token. This will be located within
        the randomly-generated file options folder of the cluster's head node

    Returns
    -------
    fs : filesystem object
        An object that references the cloud storage filesystem opened
        in the function. Passed back to `write(...)`
    """
    
    if csp == 'GCP' and bucket_type == 'Public':
        fs = gcsfs.GCSFileSystem(token='anon')

    elif csp == 'GCP' and bucket_type == 'Private':
        fs = gcsfs.GCSFileSystem(token=token)

    elif csp == 'AWS' and bucket_type == 'Public':
        fs = s3fs.S3FileSystem(anon=True)

    elif csp == 'AWS' and bucket_type == 'Private':
        fs = s3fs.S3FileSystem(anon=False, profile=credentials)

    return fs




def write(filesize : float, storage_info : dict) -> str:
    """Generates a binary file of given size.

    Parameters
    ----------
    filesize : float
        Desired size of the randomly generated binary file (in GB)
    storage_info : dict
        A dictionary containing information about all storage locations
        that randomly-generated files are to be written to.

    Returns
    -------
    full_path : str
        Full path of randomly generated file written into a provided cloud
        storage location. This information is passed back to the main workflow
        for later use, and will represent either a full URI of the object location
        or POSIX filesystem path corresponding to the location of the object if the
        storage is mounted
    """

    # Return filesize in bytes
    bytefilesize = setup(filesize)
    
    # Set filename
    filename = 'random_' + str(filesize) + 'GB.bin'


    # Write binary file to cloud storage. If bucket is mounted, write
    # file like normal. If bucket is a URI, open filesystem and write
    # to the remote location
    for n in range(len(storage_info)):

        # Grab info about current cloud storage location
        current_uri = storage_info[n]['Path']
        location = current_uri + '/cloud-data-transfer-benchmarking/randfiles'
        csp = storage_info[n]['CSP']
        credentials = storage_info[n]['Credentials'].split('/')[-1]
        bucket_type = storage_info[n]['Type']

        full_path = f'{location}/{filename}'

        match bucket_type:
            case 'PW Mounted':
                with open(full_path, 'wb') as file:
                    file.write(os.urandom(bytefilesize))

            case other:
                fs = SetFileSystem(bucket_type, csp, credentials)
                with fs.open(full_path, 'wb') as file:
                    file.write(os.urandom(bytefilesize))
                del fs


        # Print confirmation message and return file path
        print(f'File written to \"{full_path}\"')

    return filename