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

    # Merges the flight info and the ground info together and returns the joined data frame.
    def __mergeData(self, columnHeadsForMerge):

        logging.debug('Starting to merge data frames for flight info and ground info.')
        df = pd.merge(self.flightInfo, self.groundInfo, how='outer', on=columnHeadsForMerge, sort=True)
        #df = df.sort_values('n_x')

        # postprocessing: Delete all rows which do not fulfill the following criteria on having been merged solely by columns with the same names
        rowIndezesToDelete = []
        rowsDeleted = 0
        dayOfOriginColumn = df[Common.columnName_day_of_origin]
        depApSchedColumn = df[Common.columnName_dep_ap_sched]
        arrApSchedColumn = df[Common.columnName_arr_ap_sched]
        depSchedDateColumn = df[Common.columnName_dep_sched_date]
        arrSchedDateColumn = df[Common.columnName_arr_sched_date]

        for rowIndex in range(len(df)):
            try:
                if rowIndex == 0:
                    continue
                groundInfoDateString = dayOfOriginColumn.values[rowIndex]

                row = df.loc[rowIndex]

                if(groundInfoDateString == '' or groundInfoDateString is None
                        or (isinstance(groundInfoDateString, numbers.Number)
                            and math.isnan(groundInfoDateString))):
                    continue

                groundInfoDate = self.__convertToDate(groundInfoDateString)
                departureAirport = depApSchedColumn.values[rowIndex]
                arrivalAirport = arrApSchedColumn.values[rowIndex]

                compareDate = None

                if(departureAirport == Common.groundAirport):
                    compareDate = self.__convertToDate(depSchedDateColumn.values[rowIndex])
                elif(arrivalAirport == Common.groundAirport):
                    compareDate = self.__convertToDate(arrSchedDateColumn.values[rowIndex])
                else:
                    continue
                    #compareDate = self.__convertToDate(arrSchedDateColumn.values[rowIndex])

                deleteRow = compareDate.date() != groundInfoDate.date()

                if(deleteRow):
                    rowIndezesToDelete.append(rowIndex)
                    rowsDeleted = rowsDeleted + 1
            except Exception as e:
                raise Exception(f'Error while examining row with index "{rowIndex}".').with_traceback(e.__traceback__)

        if(rowsDeleted > 0):
            logging.info(f'Deleting "{rowsDeleted}" rows in joined data frame, which were not merged correctly.')
            df.drop(index=rowIndezesToDelete, axis=0, inplace=True)
        else:
            logging.Info('No rows were deleted in joined data frame. All was merged correctly.')

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