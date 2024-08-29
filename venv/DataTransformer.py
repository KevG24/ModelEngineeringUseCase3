import Common
import logging
import random
import pandas as pd
from datetime import datetime, date

class DataTransformer:

    rawData = None
    __stringRegister__ = []
    # region Constructor
    def __init__(self, data):
        if not isinstance(data, pd.DataFrame):
            raise Exception('Expected pandas data frame.')
        self.rawData = data

    # endregion Constructor

    # region Methods
    def transformData(self):

        for columnName in self.rawData.columns:

            datatype = self.__getColumnDataType(columnName)
            logging.debug(f'Identified data type "{datatype}" for column "{columnName}".')


    def __getColumnDataType(self, columnName):

        dateTimeKey = "datetime"
        intKey = "int"
        floatKey = "float"
        stringKey = "string"
        columnData = self.rawData[columnName]

        datatypedict = {dateTimeKey: 0,
                        intKey: 0,
                        floatKey: 0,
                        stringKey: 0}

        indezes = []
        numberOfValuesToCheckForDataType = 5

        # create random row indezes to check for the data type of the value
        for index in range(0, numberOfValuesToCheckForDataType):
            indezes.append(random.randint(0,len(columnData)-1))

        # iterate through the random row indezes
        for rowIndex in indezes:
            val = columnData[rowIndex]

            if (self.__isFloat(val)):
                datatypedict[floatKey] = datatypedict[floatKey] + 1
            elif(self.__isDateTime(val)):
                datatypedict[dateTimeKey] = datatypedict[dateTimeKey] + 1
            elif (self.__isInt(val)):
                datatypedict[intKey] = datatypedict[intKey] + 1
            else:
                datatypedict[stringKey] = datatypedict[stringKey] + 1

        foundValidKey = False
        foundKeyWithInconclusiveCounter = False
        validKey = stringKey

        # iterate through data type dictionary and look, if only one data type was found
        for key in datatypedict:
            if(datatypedict[key] == numberOfValuesToCheckForDataType) and foundKeyWithInconclusiveCounter == False:
                foundValidKey = True
                validKey = key
            elif(datatypedict[key] > 0):
                foundKeyWithInconclusiveCounter = True
                foundValidKey = False

        if(foundValidKey == False):
            logging.warn(f'Unable to identify conclusive data type for column "{columnName}". Taking string data type.')

        return validKey

    # region Methods for determining data types
    def __isDateTime(self, stringValue):
        try:
            dateTimeValue = datetime.strptime(stringValue, Common.dateformat)
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

    # endregion Methods