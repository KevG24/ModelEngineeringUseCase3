import datetime
import logging
import math
import pathlib

# region File Paths

flightInfoFilePath = '..\\Rohdaten\\flight_information.csv'
groundInfoFilePath = '..\\Rohdaten\\ground_information.csv'
# endregion File Paths

csvFileExtension = '.csv'

# region Thresholds
threshold_individual_values_per_column = 0.0001

# the threshold for missing values,
# e.g.: for 0.01, it means that a maximum of 1% of the values may be missing values to be filled
threshold_for_missing_values = 0.01
# endregion Thresholds

# region Logging
# initialize the file name for the log file
LogFileName = f'Logging\\LogFile_{datetime.datetime.today().strftime("%Y-%m-%d")}.log'
# initialize the logging basic configuration
logging.basicConfig(filename=LogFileName,
                    format='[%(asctime)s]: [%(levelname)s]\t - [%(pathname)s, LineNo: %(lineno)d] - %(message)s',
                    encoding='utf-8',
                    level=logging.DEBUG)
logging.info('Started new program run.')
logging.info('Initialized logging basic configuration.')

# endregion Logging
