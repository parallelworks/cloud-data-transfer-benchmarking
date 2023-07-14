import os
import gcsfs




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




def SetFileSystem(location : str, bucket_type : str, csp : str, token : str):
    """Opens a cloud storage filesystem to write to nonmounted locations

    Parameters
    ----------
    location : str
        Cloud object store URI
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
    
    # TODO: Add S3 filesystem support
    match csp:
        case 'GCP':
            fs = gcsfs.GCSFileSystem(location, token=token)
    return fs




def write(filesize : float, location : str, bucket_type : str, csp : str, token : str) -> str:
    """Generates a binary file of given size.

    Parameters
    ----------
    filesize : float
        Desired size of the randomly generated binary file (in GB)
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
        Full path of randomly generated file written into a provided cloud
        storage location. This information is passed back to the main workflow
        for later use, and will represent either a full URI of the object location
        or POSIX filesystem path corresponding to the location of the object if the
        storage is mounted
    """

    # Return filesize in bytes
    bytefilesize = setup(filesize)
    
    # Set filename
    full_path = location + 'random_' + str(filesize) + 'GB.bin'


    # Write binary file to cloud storage. If bucket is mounted, write
    # file like normal. If bucket is a URI, open filesystem and write
    # to the remote location
    match bucket_type:
        case 'PW Mounted':
            with open(full_path, 'wb') as file:
                file.write(os.urandom(bytefilesize))

        case other:
            fs = SetFileSystem(location, bucket_type, csp, token)
            with fs.open(full_path, 'wb') as file:
                file.write(os.urandom(bytefilesize))


    # Print confirmation message and return file path
    print(f'File written to \"{full_path}\"')
    return full_path