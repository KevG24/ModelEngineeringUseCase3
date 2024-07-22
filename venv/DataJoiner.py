import Common
import pandas as pd
import logging
from CsvFileHelper import CsvFileHelper

class DataJoiner:

    # region Fields
    __csvFileHelper = None
    flightInfo = None
    groundInfo = None
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
        logging.debug(f'Using the following columns for merging of both dataframes: "{(columnsForMerge)}')

        logging.info('Starting merge of data sets.')
        resultdataframe = self.__mergeData(columnsForMerge)

        filepath = Common.exportJoinDataFilePath
        logging.debug(f'Export joined data to csv file "{filepath}".')
        self.__csvFileHelper.ExportCsvFile(resultdataframe, filepath)

    def __mergeData(self, columnHeadsForMerge):

        #df = self.flightInfo.join(self.groundInfo)
        df = pd.concat([self.flightInfo, self.groundInfo], join="outer", sort=False)
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
    # endregion Methods