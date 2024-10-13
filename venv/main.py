import logging
import Common
from DataCleaner import DataCleaner
from DataJoiner import DataJoiner
from DataTransformer import DataTransformer
from ModelTrainer import ModelTrainer
from RandomForestTrainer import RandomForestTrainer

def preprocessData():
    try:
        datacleaner = DataCleaner(Common.flightInfoFilePath, Common.groundInfoFilePath)
        datacleaner.CleanData()

        datajoiner = DataJoiner(datacleaner.flightInfoRaw, datacleaner.groundInfoRaw)
        datajoiner.JoinData()

        dataTransformer = DataTransformer(datajoiner.joinedData)
        dataTransformer.transformData()

        return dataTransformer.trainData, dataTransformer.trainTargetData

    except Exception as e:
        logging.critical('Error while preprocessing data.', exc_info=e)
        raise e

def trainModel(data, targetData):
    try:
        logging.info('Starting model training.')
        modelTrainer = RandomForestTrainer(data,targetData)
        modelTrainer.train()
    except Exception as e:
        logging.critical('Error while training model.', exc_info=e)

data, targetData = preprocessData()
trainModel(data, targetData)
logging.info('Exited program run.')