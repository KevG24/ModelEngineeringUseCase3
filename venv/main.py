import logging
import Common
from DataCleaner import DataCleaner
from DataJoiner import DataJoiner

def preprocessData():
    try:
        datacleaner = DataCleaner(Common.flightInfoFilePath, Common.groundInfoFilePath)
        datacleaner.CleanData()

        datajoiner = DataJoiner(datacleaner.flightInfoRaw, datacleaner, datacleaner.groundInfoRaw)

    except Exception as e:
        logging.error('Error while executing main.', exc_info=e)

    logging.info('Exited program run.')
    return

preprocessData()
