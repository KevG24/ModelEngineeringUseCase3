import Common
import pandas as pd
import logging
from datetime import datetime, date
from CsvFileHelper import CsvFileHelper
import numbers
import math

class DataJoiner:

    # region Fields
    __csvFileHelper = None
    flightInfo = None
    groundInfo = None

    joinedData = None
    # endregion Fields

    # region Constructor
    def __init__(self, flightInfo, groundInfo):
        if not isinstance(flightInfo, pd.DataFrame) or not isinstance(groundInfo, pd.DataFrame):
            raise Exception('Expected pandas data frame.')

        self.flightInfo = flightInfo
        self.groundInfo = groundInfo

        self.__csvFileHelper = CsvFileHelper()
    # endregion Constructor

    # region Methods
    def JoinData(self):
        logging.debug('Starting to join the flight and ground info.')

        columnsForMerge = self.__getColumnNamesForMerge()
        logging.debug(f'Using the following columns for merging of both dataframes: "{(columnsForMerge)}"')

        logging.info('Starting merge of data sets.')
        self.joinedData = self.__mergeData(columnsForMerge)

        filepath = Common.exportedJoinDataFilePath
        logging.debug(f'Export joined data to csv file "{filepath}".')

        try:
            self.__csvFileHelper.ExportCsvFile(self.joinedData, filepath)
        except Exception as e:
            logging.error(f'Unable to export joined data to csv file "{filepath}".', e)

    # Merges the flight info and the ground info together and returns the joined data frame.
    def __mergeData(self, columnHeadsForMerge):

        logging.debug('Starting to merge data frames for flight info and ground info.')
        df = pd.merge(self.flightInfo, self.groundInfo, how='left', on=columnHeadsForMerge, sort=True)

        joinedDFSize = len(df)
        originalSize = len(self.flightInfo)
        if(joinedDFSize != originalSize):
            logging.warn(f'Joined data frame size "{joinedDFSize}" is larger than the flight info size "{originalSize}". Please select more columns to merge by to ensure, that both dataframe have the same size.')

        return df

    def __getColumnNamesForMerge(self):

        logging.debug('Starting to determine the columns to merge the dataframes by.')
        flightInfoColumnHeads = self.flightInfo.columns
        groundInfoColumnHeads = self.groundInfo.columns

        columnHeadsForMerge = []

        for flightInfoColumn in flightInfoColumnHeads:
            if(flightInfoColumn == "n"):
                continue
            for groundInfoColumn in groundInfoColumnHeads:
                if flightInfoColumn == groundInfoColumn:
                    columnHeadsForMerge.append(flightInfoColumn)
                    break

        return columnHeadsForMerge

    # Converts a given string into a date.
    def __convertToDate(self, stringValue):
        return datetime.strptime(stringValue, Common.dateformat)
    # endregion Methods