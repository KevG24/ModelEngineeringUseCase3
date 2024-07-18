import logging
import Common
from DataCleaner import DataCleaner

def main():
    try:
        datacleaner = DataCleaner(Common.flightInfoFilePath, Common.groundInfoFilePath)
    except Exception as e:
        logging.error('Error while executing main.', exc_info=e)

    logging.info('Exited program run.')
    return

main()
