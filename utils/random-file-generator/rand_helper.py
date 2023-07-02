""" Random File Generation Helper Classes

This script defines classes useful for processing user input and
executing writes for randomly generated files. 

It requires `gcsfs` and `s3fs` for writing to cloud storage 
locations when a URI is given, `xarray` & `scipy` for writing NetCDF4 files,
and numpy & pandas for writing CSV files. These packages must be
installed on the environment that `rand_files.py` is running in
to function correctly.

The file is not meant to be run as a script, rather imported as a module
into `rand_files.py`. It contains the following classes and custom errors:

    * preprocessor - The first code that interacts with the random
    file generation user input from `main.ipynb` and the PW workflow
    launch form

    * randfileGenerator - Generates randomly populated files of a
    size specified by the user

    * InvalidCredentialsError - raised when attemped assess of
     cloud storage credentials is not successful.

"""


import gcsfs
import s3fs
import dask
import xarray as xr
import h5netcdf
import numpy as np
import pandas as pd
import math
import os



except InvalidCredentialsError:
    """ Raised when an attempt to access the credentials was unsucessful,
    or when the credentials do not have the correct permissions."""

    print("The path the cloud storage credentials file is either " \
    "incorrect, or they do not grant write access.\n Verify that the credential" \
    "path was input correctly, or edit the credentials to include write access.")



class preprocessor:
    """
    Class used to put user input into a common form
    and determine the cloud service providers of
    user-specified cloud object stores.

    ...

    Attributes
    ----------
    storage_locations : tuple
        a tuple that contains strings referencing cloud object
        storage locations. These strings can either be URIs or
        absolute paths of cloud storage systems mounted to act
        as a POSIX-like filesystem

    Methods
    -------
    verify_path(storage_loc : str)
        Edits a storage location string if it does not
        end with a terminating `/`. Can be run on a case-
        by-case basis (a single string at a time)
    set_csp(storage_loc : str)
        Determines the cloud service provider of cloud
        storage location string that contains a URI.
        Can be run for one string at a time
    process()
        Executes the above two methods for the entire tuple
        of cloud object storage locations
    """

    def __init__(self, storage_locations : tuple, filetypes : tuple(str)):
        """
        Parameters
        ----------
        storage_locations : tuple
            Tuple of cloud storage URIs or absolute path
            of cloud object store mounted into POSIX filesystem
        filetype : str
            A tuple of strings containing the desired filetypes
            to randomly generate. Will only contain one or a
            combination of the following formats: CSV, NetCDF4, and Binary
        """

        self.storage_locs = storage_locations
        self.storage_locs_new = []
        self.csp = []


    def verify_path(self, storage_loc : str) -> str:
        """Checks for a string to have a terminating `/`.
         If there are none, appends the character.
        
        Parameters
        ----------
        storage_loc : str
            A single string of a cloud object store URI
            or absolute path of a mounted object store

        Returns
        -------
        storage_loc_new : str
            A string containing the edited cloud object
            store name
        """

        if storage_loc[-1] != '/':
            storage_loc = storage_loc + '/'
        return storage_loc_new


    def set_csp(self, storage_loc : str) -> str:
        """Assigns a string corresponding to
        a cloud service provider affiliated with
        the cloud object store URI. Assignment is
        based on the first character of the string.

        Parameters
        ----------
        storage_loc : str
            A single string of a cloud object store URI
            or absolute path of a mounted object store.
            Does not need to be in the general form obtained
            in the previous method. 

        Raises
        ------
        NonImplementedError
            If the first character of the string does not
            match any of the given cases, assume that the
            given cloud service provider is not yet supported

        Returns
        -------
        csp : str
            A string of the cloud service provider
        """

        first_character = storage_loc[0]

        # TODO: Find a better way to capture all CSP cases.
        # Public stores often start with `http://` and would
        # rasie an error if used in the current configuration
        # of the workflow.
        match first_character:
            case "g":
                csp = 'GCP'
            case "s":
                csp = 'AWS'
            case "/"
                csp = None
            case other:
                raise NotImplementedError("Given cloud service provider" \
                " is not yet supported by workflow.)
        return csp


    def process(self):
        """Performs all preprocessing step for entire
        set of cloud storage location strings. This method
        should be the only one run in the entire class if
        performing the preprocessing step during the workflow.
        The other methods should only be run if interested in
        inspecting a specific CSP or edited location string.

        Returns
        -------
        dict('Locations': tuple, 'CSPs': tuple)
            Dictionary containing the edited cloud object storage
            strings and respective cloud serivce provider identifiers
        """

        for i in range(len(self.storage_locs)):
            current_loc = self.storage_locs[i]

            # Run preprocessing functions
            current_loc = self.verify_path(current_loc)
            current_csp = self.set_csp(current_loc)

            # Record strings in current loop iteration
            self.storage_locs_new.append(current_loc)
            self.csp.append(current_csp)

        final_locations = tuple(self.storage_locs_new)
        final_csps = tuple(self.csp)

        return {'Locations': final_locations, 'CSPs': final_csps}



class randfileGenerator:
    """
    Class used to write files with randomly generated
    values of a given GB size. In order to write files of
    arbitrarily large size, we use Dask to handle out-of-memory
    data sizes

    Methods
    -------
    getInput(filesize : float, location : str, token : str, csp : str)
        Gets all required input for the file generator object
    filesystems()
        Cannot be called from outside of the class. Helper method used
        to set a filesystem to write binary and NetCDF files if URI of
        a cloud object store is provided as the write location.
    confirmation(name : str)
        Helper method called from other methods in the class used to
        print a confirmation method upon a successful write.
    """

    def __init__(self):
        "Initialize variables used across all file generation methods"
        np.random.seed(0)
        self.basename = 'randfile'
        self.GB2Byte = 1073741824


    def getInput(self, filesize : float, location : str, token : str, csp : str):
        """First method that should be called upon instantiation of the class.
        All input to the class is passed into this method.

        Parameters
        ----------
        filesize : float
            Size (in GB) of the file to be randomly generated
        location : str
            Cloud object store URI or absolute path of a cloud store
            mounted in an accessable POSIX filesystem (PW storage
            feature)
        token : str
            Absolute path to the cloud object store credential token
            corresponding to the location parameter
        csp : str
            Cloud service provider identifier created by`preprocessor'
        """

        self.filesize = filesize
        self.location = location
        self.token = token
        self.csp = csp


    def filesystems(self):
        """Helper method used to access cloud object stores for use in writing
        NetCDF and binary files. Should not be called outside of the class.

        Raises
        ------
        NonImplementedError
            Raised if the cloud storage provider is set to AWS. Issues with accessing
            AWS in a general way have arisen, so this feature will be added once a
            working version of the workflow with Google has been tested and uploaded.
        """

        match self.csp:
            case 'GCP':
                self.fs = gcsfs.GCSFileSystem(self.location, token=self.token)
            case 'AWS':
                raise NotImplementedError("Writes to S3 storage using a URI are not " \
                "yet supported by the workflow.")
    

    def confirmation(self, name : str):
        "Prints confirmation message to the user terminal when file is written"
        print(f'\"{name}\" written to \"{self.location}\"')


    def csv(self) -> str:
        """Generates CSV file using inputs from `getInput` method.

            Raises
            ------
            InvalidCredentialsError
                Raised if path to cloud storage credentials are incorrectly
                input by the user or if the given credentials file does not
                have the correct permissions

            Returns
            -------
            full_path : str
                Full path of randomly generated file written into a provided cloud
                storage location. This information is passed back to the main workflow
                for later use, and will represent either a full URI of the object location
                or POSIX filesystem path corresponding to the locaton of the object if the
                storage is mounted
        """

        print('Generating CSV...')

        n_b = 19
        c_b = 1
        df_size = round((c_b+math.sqrt(c_b**2 + 4*(c_b+n_b)*(self.GB2Byte*self.filesize)))/(2*(n_b+c_b)))
        df = pd.DataFrame(np.random.randn(df_size,df_size))

        name = self.basename + '_' + str(self.filesize) + 'GB.csv'
        full_path = self.location + name

        if self.csp == None:
            df.to_csv(full_path, header=None, index=False)
        else:
            try:
                df.to_csv(full_path, header=None, index=False, storage_options={'token':token})
            except:
                raise InvalidCredentialsError

        self.confirmation(name)
        return full_path


    def netcdf(self, float_dims : int, time_dims : int) -> str:
        """Generates NetCDF4 file of a given size. The generated NetCDF4 file
        may have any number of dimensions that a user wishes, provided that
        the overhead of creating a file does not exceed the file size.

        Parameters
        ----------
        float_dims : int
            The desired number of float-valued dimensions to use in NetCDF4 generation
        time_dims : int
            The desired number of time-valued dimensions to use in NetCDF4 generation

        Returns
        -------
        full_path : str        
            Full path of randomly generated file written into a provided cloud
            storage location. This information is passed back to the main workflow
            for later use, and will represent either a full URI of the object location
            or POSIX filesystem path corresponding to the locaton of the object if the
            storage is mounted
        """

        name = self.basename + '_' + str(self.filesize) + 'GB.nc'
        full_path = self.location + name
        self.filesystems() # Call the filesystems() method and set
                           # the correct filesystem based on the CSP

        # Determine NetCDF4 file size
        value_bytes = 8 # four bytes for each float value in the data variable
        coord_bytes = 8 # four bytes for each integer coordinate value
        time_bytes = 8 # eight bytes for each time value
        n_dims = float_dims + time_dims # Total number of desired dimensions

        # Populate polynomial coefficients list
        coeffs = [value_bytes] # First term of polynomial will always be 4
        for i in range(n_dims-2):
            coeffs.append(0) # All coefficents of polynomial terms with
                             # order n-1 to 2 will be zero
        coeffs.append(coord_bytes*float_dims + time_bytes*time_dims) # Dimension coefficient
        coeffs.append(-self.filesize*self.GB2Byte + overhead) # Final term representing filesize & overhead

        # Find only positive roots and determine
        # the integer representing coordinate length
        sols = np.roots(coeffs)
        for i in sols:
            if i.imag == 0 and i.real > 0:
                dim_length = round(i.real)
        


        

        

        # TODO: Generalize NetCDF4 writing for a given size
        ds = xr.Dataset(
            {"random": (("x", "y", "z"), np.random.rand(4,4,4))},
            coords = {
                "x": [10, 20, 30, 40],
                "y": [10, 20, 30, 40],
                "z": pd.data_range("2023-01-01", periods = 4),
            },
        )
        match self.csp:
            case None:
                ds.to_netcdf(full_path, engine=h5netcdf)
            case other:
                with self.fs.open(full_path, 'wb') as file:
                    ds.to_netcdf(file, engine=h5netcdf)

        self.confirmation(name) # Call the confirmation(...) attribute
        return full_path


    def binary(self) -> str:
        """Generates a binary file of given size.

        Returns
        -------
        full_path : str
            Full path of randomly generated file written into a provided cloud
            storage location. This information is passed back to the main workflow
            for later use, and will represent either a full URI of the object location
            or POSIX filesystem path corresponding to the locaton of the object if the
            storage is mounted
        """

        print('Generating binary file...')

        # Get requested file size in bytes and set file name
        bytefilesize = round(self.GB2Byte*self.filesize)
        name = self.basename + '_' + str(self.filesize) + 'GB.dat'
        full_path = self.location + name
        self.filesystems()

        match self.csp:
            case None:
                with open(full_path, 'wb') as file:
                    file.write(os.urandom(bytefilesize))
            case other:
                with self.fs.open(full_path, 'wb') as file:
                    file.write(os.urandom(bytefilesize))
                
        self.confirmation(name)
        return full_path