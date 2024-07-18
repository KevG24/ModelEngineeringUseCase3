import logging
from os.path import isfile
from pathlib import Path
import Common

import pandas as pd

class CsvFileHelper:

    # region Functions

    # region Private

    # Determines whether the given filepath is a valid filepath.
    def __IsFilePathValid(self, filepath):
        ''' Check if the file at filepath exists
            also check if it has the file extension .csv '''
        if (isfile(filepath) == True and Path(filepath).suffix == Common.csvFileExtension):
            return True
        return False

    # endregion Private

    # region Public

    # Reads data from a csv file
    def ReadFromCsvFile(self, filepath):
        try:
            ''' Check if the filepath leads to a valid csv file.
                If not, throw an exception.'''
            if (self.__IsFilePathValid(filepath) == False):
                raise Exception(f'No valid csv file found at path "{filepath}".')
            ''' Reads the data from the csv file (filepath)'''
            data = pd.read_csv(filepath)

            logging.debug(f'Successfully imported data from csv file "{filepath}" of structure "{data.shape}".')
            return data
        except Exception as e:
            raise Exception(f'Error while reading csv file "{filepath}".', e)
    # endregion Public

    # endregion Functions
