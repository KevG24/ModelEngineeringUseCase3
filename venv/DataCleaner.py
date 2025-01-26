import datetime
import numbers
from datetime import datetime, date
import Common
import pandas as pd
import logging
import numpy
from Common import columns_to_merge
from numpy.f2py.auxfuncs import throw_error
from scipy.stats import entropy
import math
from CsvFileHelper import CsvFileHelper
from datetime import datetime, date

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

        # rename columns in both dataframes
        # this is meant for easier merging in the data joiner
        self.__renameColumns(self.flightInfoRaw)
        self.__renameColumns(self.groundInfoRaw)

        # remove duplicate rows in both dataframes
        self.__removeDuplicates(self.flightInfoRaw, Common.flightInfo_columnNames_to_identify_duplicate_rows)
        self.__removeDuplicates(self.groundInfoRaw, Common.groundInfo_columnNames_to_identify_duplicate_rows)

        logging.info(f'Starting to check for gaps in flight info.')
        self.__checkAndDealWithGapsInData(self.flightInfoRaw)
        logging.info(f'Starting to check for gaps in ground info.')
        self.__checkAndDealWithGapsInData(self.groundInfoRaw)

        logging.info('Checking for columns to merge in flight info.')
        self.__mergeColumns(self.flightInfoRaw)
        logging.info('Checking for columns to merge in ground info.')
        self.__mergeColumns(self.groundInfoRaw)

        # performs consistency checks on the data sets and removes inconsistent data
        self.__performConsistencyCheck()


    def __removeDuplicates(self, df, columnNamesForIdentifyDuplicates):
        if not isinstance(df, pd.DataFrame):
            raise Exception('Expected pandas data frame.')

        bool_results = df.duplicated(keep='last', subset=columnNamesForIdentifyDuplicates)
        duplicate_count = sum(bool(x) for x in bool_results)

        if(duplicate_count > 0):
            logging.info(f'Found "{duplicate_count}" duplicates. Starting to remove these duplicates.')
            indezes = []

            for i in range(len(bool_results)):
                if bool_results[i] == True:
                    indezes.append(i)

            df.drop(axis=0, index=indezes, inplace=True)
            logging.info(f'Successfully removed "{duplicate_count}" duplicate rows.')

    # This method performs consistency checks on the data sets to provide only valid information.
    def __performConsistencyCheck(self):
        logging.info('Starting conistency checks on flight and ground info data sets.')
        self.__performConistencyCheckForFlightInformation()
        self.__performConistencyCheckForGroundInformation()
        logging.info('Finished consistency checks.')

    # This method performs consistency checks for flight information data set.
    def __performConistencyCheckForFlightInformation(self):

        indezes = []
        logging.debug('Starting consistency check for flight information data set.')
        for index, row in self.flightInfoRaw.iterrows():
            try:
                # check if m_offblockdt is greater than m_onblockdt
                if(self.__convertToDateTime(row[Common.columnName_m_offblockdt]) >=
                        self.__convertToDateTime(row[Common.columnName_m_onblockdt])):
                    logging.debug(f'Detected later off block datetime than on block datetime in row with index [{index}].')
                    indezes.append(index)

                # check if departure date is equal or greater than arrival date
                if (self.__convertToDateTime(row[Common.columnName_dep_sched_date], False) >=
                        self.__convertToDateTime(row[Common.columnName_arr_sched_date], False)):
                    logging.debug(f'Detected later departure datetime than arrival datetime in row with index [{index}].')
                    indezes.append(index)

                if (row[Common.columnName_dep_ap_sched] == row[Common.columnName_arr_ap_sched]):
                    logging.debug(f'Detected equal arrival and departure airport in row with index [{index}].')
                    indezes.append(index)
            except:
                logging.error(f'Error while checking data conistency on row [{index}].')
                raise

        if(len(indezes) > 0):
            logging.info(f'Found rows to remove due to inconsistent data (flight information): [{indezes}]')
            self.groundInfoRaw.drop(axis=0, index=indezes, inplace=True)

    # This method performs consistency checks for ground information data set.
    def __performConistencyCheckForGroundInformation(self):
        indezes = []

        logging.debug('Starting consistency check for ground information data set.')
        for index, row in self.groundInfoRaw.iterrows():

            # check if m_offblockdt is greater than m_onblockdt
            if (self.__convertToDateTime(row[Common.columnName_sched_inbound_dep]) >=
                    self.__convertToDateTime(row[Common.columnName_sched_inbound_arr])):
                logging.debug(f'Detected later off departure datetime than arrival datetime in row with index [{index}].')
                indezes.append(index)

            if (self.__convertToDateTime(row[Common.columnName_sched_outbound_dep]) >=
                    self.__convertToDateTime(row[Common.columnName_sched_outbound_arr])):
                logging.debug(f'Detected later off departure datetime than arrival datetime in row with index [{index}].')
                indezes.append(index)

            if (row[Common.columnName_arr_leg_inbound] != Common.groundAirport):
                logging.debug(
                    f'Found row with wrong inbound airport on index {index}. Removing row from ground info data set.')
                indezes.append(index)

        if (len(indezes) > 0):
            logging.info(f'Found rows to remove due to inconsistent data (ground information): [{indezes}]')
            self.groundInfoRaw.drop(axis=0, index=indezes, inplace=True)

    # merges columns together by the columns_to_merge dict in Common.py
    def __mergeColumns(self, df):
        if not isinstance(df, pd.DataFrame):
            raise Exception('Expected pandas data frame.')

        columns_to_merge_dict = {}
        # check if any columns to merge exist in this dataframe
        for column in Common.columns_to_merge:
            if(column in df.columns and Common.columns_to_merge[column] in df.columns):
                columns_to_merge_dict.update({column: Common.columns_to_merge[column]})

        # if no columns found to merge, skip here
        if(len(columns_to_merge_dict) == 0):
            logging.info('No columns found to merge. Skipping merge.')
            return

        logging.info(f'Found columns to merge: "{columns_to_merge_dict}".')
        logging.info('Starting merge of columns.')

        # reset index here to prevent out-of-bound-exception on data frame iterrows()
        df.reset_index(drop=True, inplace=True)

        # iter through all rows to merge the values of the merge columns
        for index, row in df.iterrows():
            for mergeColumn in columns_to_merge_dict:
                mergeColumnToRemove = columns_to_merge_dict[mergeColumn]
                try:
                    # put the value of mergeColumn together with the mergeColumnToRemove
                    # the column of mergeColumnToRemove is later to be removed entirely
                    newValue = row[mergeColumn] + Common.colums_to_merge_value_separator + row[mergeColumnToRemove]
                    df[mergeColumn].values[index] = newValue
                except:
                    logging.error(f'Error while merging columns "{mergeColumn}" and "{mergeColumnToRemove}" on row index "{index}".')
                    raise

        # drop all merged columns, which were marked as to be removed, 'mergeColumnToRemove'
        for mergeColumn in columns_to_merge_dict:
            df.drop(columns_to_merge_dict[mergeColumn], axis=1, inplace=True)

    # Renames columns in given dataframe by the Common.columns_to_rename dictionary.
    def __renameColumns(self, df):
        if not isinstance(df, pd.DataFrame):
            raise Exception('Expected pandas data frame.')

        dfColumnNames = df.columns

        for columnName in Common.columns_to_rename:
            renameColumnName = Common.columns_to_rename[columnName]

            for dfColumnName in dfColumnNames:
                if dfColumnName == columnName:
                    logging.debug(f'Renaming column "{dfColumnName}" to "{renameColumnName}".')
                    df.rename(mapper={dfColumnName: renameColumnName}, inplace=True, axis=1)


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

                if(keepColumn & Common.columns_to_remove.__contains__(columnName) == False):
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

        return individual_values > Common.threshold_individual_values_per_column

    # Converts a given string into a date time.
    def __convertToDateTime(self, stringValue, withSeconds = True):
        try:
            format = Common.datetimeformat if withSeconds else Common.datetimeformatWithoutSeconds
            return datetime.strptime(stringValue, format)
        except:
            return self.__convertToDate(stringValue)

    # Converts a given string into a date.
    def __convertToDate(self, stringValue):
        return datetime.strptime(stringValue, Common.datetimeformatWithoutSeconds)


