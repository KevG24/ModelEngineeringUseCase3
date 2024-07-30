import logging
import Common
from DataCleaner import DataCleaner
from DataJoiner import DataJoiner

def preprocessData():
    try:
        datacleaner = DataCleaner(Common.flightInfoFilePath, Common.groundInfoFilePath)
        datacleaner.CleanData()

        datajoiner = DataJoiner(datacleaner.flightInfoRaw, datacleaner.groundInfoRaw)
        datajoiner.JoinData()

    except Exception as e:
        logging.critical('Error while executing main.', exc_info=e)
    finally:
        logging.info('Exited program run.')
    return

preprocessData()
