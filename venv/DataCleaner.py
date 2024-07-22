import datetime
import numbers
from datetime import datetime, date
import Common
import pandas as pd
import logging
import numpy
from scipy.stats import entropy
import math
from CsvFileHelper import CsvFileHelper

class DataCleaner:
    # region Fields
    flightInfoRaw = None
    groundInfoRaw = None
    # endregion Fields

    # Constructor
    def __init__(self, flightInfoFilePath, groundInfoFilePath):
        csvFileHelper = CsvFileHelper()
        logging.info(f'Reading flight info from file path "{flightInfoFilePath}.')
        self.flightInfoRaw = csvFileHelper.ReadFromCsvFile(flightInfoFilePath)
        logging.info(f'Reading ground info from file path "{groundInfoFilePath}.')
        self.groundInfoRaw = csvFileHelper.ReadFromCsvFile(groundInfoFilePath)

    def CleanData(self):
        self.__removeColumnsWithoutInformation__(self.flightInfoRaw)
        self.__removeColumnsWithoutInformation__(self.groundInfoRaw)

        logging.info(f'Starting to check for gaps in flight info.')
        self.__checkAndDealWithGapsInData(self.flightInfoRaw)
        logging.info(f'Starting to check for gaps in ground info.')
        self.__checkAndDealWithGapsInData(self.groundInfoRaw)

    # This method identifies gaps in the data and removes (if necessary) inconsistent data
    def __checkAndDealWithGapsInData(self, df):
        if not isinstance(df, pd.DataFrame):
            raise Exception('Expected pandas data frame.')

        for column in df.columns:
            try:
                arr = df[column].to_numpy()
                numberOfGaps = self.__getCountOfGaps(arr)
                logging.debug(f'"{numberOfGaps}" gaps in column "{column}" found.')

                # display the percentage of missing values for the column
                if(numberOfGaps > 0):
                    missingvalues_ratio = numberOfGaps/len(arr)
                    logging.info(f'The column "{column}" consists of "%.2f%%" missing values.', missingvalues_ratio*100)

                    # if the missing values ratio is higher than the threshold, remove the column
                    if(missingvalues_ratio > Common.threshold_for_missing_values):
                        logging.warn(f'The column "{column}" has more missing values than allowed. The column will be removed.')
                        df.drop(column, axis=1, inplace=True)
                    elif(missingvalues_ratio > 0):
                        logging.info(f'Starting to fill the gaps in column "{column}".')
                        self.__fillGapsInColumn(df, column)
            except Exception as e:
                raise Exception('Error on column iterating.', e)

    def __getCountOfGaps(self, arr):
        if not isinstance(arr, numpy.ndarray):
            raise Exception('Expected numpy array.')
        counter = 0
        for i in arr:
            # check for missing values and count them
            if i is None or (isinstance(i, numbers.Number) and math.isnan(i)):
                counter = counter + 1
        return counter

    def __fillGapsInColumn(self, df, columnName):
        if not isinstance(df, pd.DataFrame):
            raise Exception('Expected pandas data frame.')

        column = df[columnName]

        for rowIndex in range(len(df)):
            val = column.values[rowIndex]
            # if a gap (empty value) is found
            if(val is None or (isinstance(val, numbers.Number) and math.isnan(val))):
                logging.debug(f'Found gap in column "{columnName}" at line "{rowIndex}".')
                self.__fillSingleGap(df, rowIndex, columnName)

    def __fillSingleGap(self, df, rowIndex, columnName):
        if not isinstance(df, pd.DataFrame):
            raise Exception('Expected pandas data frame.')
        if not isinstance(rowIndex, numbers.Number):
            raise Exception('Expected number as row index.')

        match columnName:
            # case for column "m_onblockdt"
            case Common.columnName_m_onblockdt:
                logging.debug(f'Starting to fill gap in line')

                # strategy: take the value "m_offblockdt" and add the estimated duration of the flight on it.
                offblockdt = datetime.strptime(df[Common.columnName_m_offblockdt].values[rowIndex], Common.datetimeformat)

                dep_sched_time = datetime.strptime(df[Common.columnName_dep_sched_time].values[rowIndex], Common.timeformat)
                arr_sched_time = datetime.strptime(df[Common.columnName_arr_sched_time].values[rowIndex], Common.timeformat)

                flight_time = arr_sched_time - dep_sched_time
                onblockdt = offblockdt + flight_time

                df[Common.columnName_m_onblockdt].values[rowIndex] = onblockdt.strftime(Common.datetimeformat)

                logging.debug(f'Calculated "{onblockdt}" for missing on_blockdt value in row "{rowIndex}".')

    def __removeColumnsWithoutInformation__(self, df):
        if not isinstance(df, pd.DataFrame):
            raise Exception('Expected pandas data frame.')
        logging.debug('Starting to detect columns without information.')
        indezesToRemove = []
        for i in range(1, df.shape[1]):
            try:
                column = df.iloc[:,i]
                columnName = df.columns.tolist()[i]
                keepColumn = self.__hasColumnInfoToKeep__(column.to_frame())

                if(keepColumn):
                    logging.info(f'Column "{columnName}" is set to be kept.')
                else:
                    logging.warn(f'Column "{columnName}" is set to be removed.')
                    indezesToRemove.append(columnName)
            except Exception as e:
                raise Exception(f'Unable to analyze column "{column[0]}" on iter = {i}.', e)

        for columnName in indezesToRemove:
            df.drop(columnName, axis=1, inplace=True)
            logging.debug(f'Removed column with name "{columnName}".')

    # This method checks a data frame (which should consist of only one column), if its information should be kept or not.
    def __hasColumnInfoToKeep__(self, data):
        if not isinstance(data, pd.DataFrame):
            raise Exception('Expected pandas data frame.')

        entries = data.value_counts()

        individual_values = len(entries) / len(data)

        if(individual_values > Common.threshold_individual_values_per_column):
            return True
        else:
            return False


