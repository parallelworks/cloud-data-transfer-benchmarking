"""This script defines a few commonly-used classes within the benchmarking.
These include:

    * DiagnosticTimer
        A class used to time operations and record the data in a Pandas dataframe
    * DevNullStore
        A class that creates a Python dictionary that forgets whatever is put in it.
        Similar to porting files to `/dev/null`
"""


from contextlib import contextmanager
import pandas as pd
import time
import os


class DiagnosticTimer:
    """
    This class is used to time a wide range of operations
        in the benchmarking: preprocessing, reading, performing
        mathematical operations, etc. It stores the data 

    Attributes
    ----------
    time_desc:
        A string used to describe what the timing data is measuring

    Methods
    -------
    time():
        Measures the time it take to execute any commands coded in
        after its call

        Sample call:
            with DiagnosticTimer.time(**kwargs):
                <python commands>

    dataframe():
        Creates and returns a Pandas dataframe containing timing 
        data and other keyword arguments specified by the user
    """

    def __init__(self, time_desc='Elapsed Time'):
        """This class is used to time a wide range of operations
        in the benchmarking: preprocessing, reading, performing
        mathematical operations, etc. It stores the data 

        Parameters
        ----------
        time_desc : str (default = 'Elapsed Time')
            Describes which operation is being timed. For example,
            a good title for the time it takes for data to be read
            from the cloud might be `time_desc='
        """
        self.time_desc = time_desc
        self.diagnostics = []
        
    @contextmanager
    def time(self, **kwargs):
        """Records execution time for Python commands
        executed under its call

        Parameters
        ----------
        **kwargs : kwarg
            Pass any number of keyword arguments to be recorded
            in a list
        """
        tic = time.time()
        yield
        toc = time.time()
        kwargs[self.time_desc] = toc - tic
        self.diagnostics.append(kwargs)
        
    def dataframe(self):
        """Populates a Pandas dataframe with keyword arguments
        that include the timing data measured with the `time()`
        attribute

        Returns
        -------
        df : Pandas dataframe
            A Pandas dataframe containing all keyword
            arguments and timing data
        """
        df = pd.DataFrame(self.diagnostics)
        return df


class DevNullStore:
    """
    A class that creates a Python
    dictionary that forgets whatever
    is put into it
    """
    def __init__(self):
        pass
    def __setitem__(*args, **kwargs):
        pass
