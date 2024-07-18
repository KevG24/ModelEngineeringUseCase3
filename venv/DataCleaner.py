import pandas as pd
import logging
from CsvFileHelper import CsvFileHelper

class DataCleaner:
    # region Fields
    csvFileHelper = None
    flightInfoRaw = None
    groundInfoRaw = None
    # endregion Fields

    def __init__(self, flightInfoFilePath, groundInfoFilePath):
        self.csvFileHelper = CsvFileHelper()
        logging.info(f'Reading flight info from file path "{flightInfoFilePath}.')
        self.flightInfoRaw = self.csvFileHelper.ReadFromCsvFile(flightInfoFilePath)
        logging.info(f'Reading ground info from file path "{groundInfoFilePath}.')
        self.groundInfoRaw = self.csvFileHelper.ReadFromCsvFile(groundInfoFilePath)