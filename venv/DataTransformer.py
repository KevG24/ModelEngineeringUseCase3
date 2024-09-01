import Common
import logging
import random
import pandas as pd
import numbers
import math
from CsvFileHelper import CsvFileHelper
from datetime import datetime, date

class DataTransformer:

    # region Constants
    dateTimeKey = "datetime"
    intKey = "int"
    floatKey = "float"
    stringKey = "string"
    # endregion Constants

    # region Private Fields
    rawData = None
    __stringValueIntKeyDict = {}
    __csvFileHelper = None
    # endregion Private Fields

    # region Constructor
    def __init__(self, data):
        if not isinstance(data, pd.DataFrame):
            raise Exception('Expected pandas data frame.')
        self.rawData = data
        self.__csvFileHelper = CsvFileHelper()

    # endregion Constructor

    # region Methods

    # transforms the data to prep the raw data for model training
    def transformData(self):
        columnDataTypeDict = {}

        # determine the data type for each column
        for columnName in self.rawData.columns:
            datatype = self.__getColumnDataType(columnName)
            logging.debug(f'Identified data type "{datatype}" for column "{columnName}".')
            columnDataTypeDict.update({columnName: datatype})

        stringDataTypeColumnNames = dict((key,value) for key, value in columnDataTypeDict.items() if value == self.stringKey)

        self.__transformStringColumnValues(list(stringDataTypeColumnNames.keys()))

        filepath = Common.transformedDataFilePath
        logging.debug(f'Export transformed data to csv file "{filepath}".')

        try:
            self.__csvFileHelper.ExportCsvFile(self.rawData, filepath)
        except Exception as e:
            logging.error(f'Unable to export transformed data to csv file "{filepath}".', e)


    # region Methods for determining data types

    # determine the data type of a column by the given column name of the raw data
    def __getColumnDataType(self, columnName):

        columnData = self.rawData[columnName]

        datatypedict = {self.dateTimeKey: 0,
                        self.intKey: 0,
                        self.floatKey: 0,
                        self.stringKey: 0}

        indezes = []
        numberOfValuesToCheckForDataType = 5

        # create random row indezes to check for the data type of the value
        for index in range(0, numberOfValuesToCheckForDataType):
            indezes.append(random.randint(0,len(columnData)-1))

        # iterate through the random row indezes
        for rowIndex in indezes:
            val = columnData[rowIndex]

            # if val is none, increase row index till you find a valid value to examine
            while(val is None or (isinstance(val, numbers.Number) and math.isnan(val)) and rowIndex < len(columnData)):
                val = columnData[rowIndex]
                rowIndex = rowIndex + 1

            # check the value for its data type and enhance the counter of the corresponding dict key value pair
            if (self.__isFloat(val)):
                datatypedict[self.floatKey] = datatypedict[self.floatKey] + 1
            elif(self.__isDateTime(val) or self.__isDate(val) or self.__isTime(val)):
                datatypedict[self.dateTimeKey] = datatypedict[self.dateTimeKey] + 1
            elif (self.__isInt(val)):
                datatypedict[self.intKey] = datatypedict[self.intKey] + 1
            else:
                datatypedict[self.stringKey] = datatypedict[self.stringKey] + 1

        foundValidKey = False
        foundKeyWithInconclusiveCounter = False
        validKey = self.stringKey
        inconclusiveKey = self.stringKey

        # iterate through data type dictionary and look, if only one data type was found
        for key in datatypedict:
            if(datatypedict[key] == numberOfValuesToCheckForDataType) and foundKeyWithInconclusiveCounter == False:
                foundValidKey = True
                validKey = key
            elif(datatypedict[key] > 0 and foundKeyWithInconclusiveCounter == False):
                foundKeyWithInconclusiveCounter = True
                inconclusiveKey = key
                foundValidKey = False

        sumOfDictValues = sum(datatypedict.values())

        if(foundValidKey == False and sumOfDictValues == numberOfValuesToCheckForDataType):
            logging.warn(f'Unable to identify conclusive data type for column "{columnName}". Taking string data type.')
        elif(foundKeyWithInconclusiveCounter == True):
            logging.warn(f'Did find values of "None".')
            validKey = inconclusiveKey
            logging.info(f'Taking inconclusive key "{inconclusiveKey}" as valid key.')

        return validKey

    def __isDateTime(self, stringValue):
        try:
            dateTimeValue = datetime.strptime(stringValue, Common.datetimeformat)
            return True
        except:
            return False
    def __isDate(self, stringValue):
        try:
            dateTimeValue = datetime.strptime(stringValue, Common.dateformat)
            return True
        except:
            return False
    def __isTime(self, stringValue):
        try:
            dateTimeValue = datetime.strptime(stringValue, Common.timeformat)
            return True
        except:
            return False

    def __isInt(self, stringValue):
        try:
            dateTimeValue = int(stringValue)
            return True
        except:
            return False

    def __isFloat(self, stringValue):
        try:
            floatNumber = float(stringValue)
            return True
        except:
            return False
    # endregion Methods for determining data types

    # region Transform methods

    #def __transformDateTimeColumnValues(self):

    # This method transforms the string values to integer values in the raw data
    def __transformStringColumnValues(self, columns=[]):

        index = 1
        self.__stringValueIntKeyDict = {}

        for index, row in self.rawData.iterrows():
            for column in columns:
                val = row.loc[column]

                if(val is None):
                    continue
                intKey = self.__getIntKeyForStringValue(val)
                self.rawData[column].values[index] = intKey

        logging.info('Finished transformation of string values.')

    # checks if the given string value already exists in the __stringValueIntKeyDict
    # if so, the value is returned
    # otherwise, the given string value is added to the dict and its value is returned
    def __getIntKeyForStringValue(self, stringValue):

        if(stringValue in self.__stringValueIntKeyDict):
            return self.__stringValueIntKeyDict[stringValue]

        index = len(self.__stringValueIntKeyDict) + 1

        self.__stringValueIntKeyDict.update({stringValue: index})
        return index

    def __transformDateTimeValues(self, columns=[]):
        if(len(columns) == 0):
            return

            for index, row in self.rawData.iterrows():
                for column in columns:
                    logging.info('temp test')
    # endregion Transform methods

    # endregion Methods