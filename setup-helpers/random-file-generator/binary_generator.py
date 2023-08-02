import os
import fsspec
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
        base_uri = storage_info[n]['Path']
        location = current_uri + '/cloud-data-transfer-benchmarking/randfiles'
        csp = storage_info[n]['CSP']
        bucket_type = storage_info[n]['Type']
        
        home = os.path.expanduser('~')
        key_dir = f'{home}/cloud-data-transfer-benchmarking/storage-keys'
        if csp == 'GCP' and bucket_type == Private:
            tmp_crds = storage_info[n]['Credentials']['token'].split('/')[-1]
            storage_info[n]['Credentials']['token'] = f'{key_dir}/{tmp_crds}'

        storage_options = storage_info[n]['Credentials']

        full_path = f'{location}/{filename}'

        match bucket_type:
            case 'PW Mounted':
                with open(full_path, 'wb') as file:
                    file.write(os.urandom(bytefilesize))

            case other:
                fs = fsspec.filesystem(base_uri.split(':')[0], **storage_options)
                with fs.open(full_path, 'wb') as file:
                    file.write(os.urandom(bytefilesize))
                del fs


        # Print confirmation message and return file path
        print(f'File written to \"{full_path}\"')

    return filename