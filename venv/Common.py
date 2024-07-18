import datetime
import logging
import math
import pathlib

# region File Paths

flightInfoFilePath = '..\\Rohdaten\\flight_information.csv'
groundInfoFilePath = '..\\Rohdaten\\ground_information.csv'
# endregion File Paths

csvFileExtension = '.csv'

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
